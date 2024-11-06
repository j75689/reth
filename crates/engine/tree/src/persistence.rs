use crate::metrics::PersistenceMetrics;
use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use reth_chain_state::ExecutedBlock;
use reth_errors::ProviderError;
use reth_primitives::GotExpected;
use reth_provider::{
    providers::{ConsistentDbView, ProviderNodeTypes},
    writer::UnifiedStorageWriter,
    BlockHashReader, BlockReader, ChainStateBlockWriter, DatabaseProviderFactory, ProviderFactory,
    StateProviderFactory, StateReader, StaticFileProviderFactory,
};
use reth_prune::{PrunerError, PrunerOutput, PrunerWithFactory};
use reth_stages_api::{MetricEvent, MetricEventsSender};
use reth_trie::{updates::TrieUpdates, HashedPostState, TrieInput};
use reth_trie_parallel::parallel_root::{ParallelStateRoot, ParallelStateRootError};
use std::{
    sync::mpsc::{Receiver, SendError, Sender},
    time::Instant,
};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, error};

/// Writes parts of reth's in memory tree state to the database and static files.
///
/// This is meant to be a spawned service that listens for various incoming persistence operations,
/// performing those actions on disk, and returning the result in a channel.
///
/// This should be spawned in its own thread with [`std::thread::spawn`], since this performs
/// blocking I/O operations in an endless loop.
#[derive(Debug)]
pub struct PersistenceService<N: ProviderNodeTypes, P> {
    /// The provider factory to use
    provider: ProviderFactory<N>,
    /// The view provider
    view_provider: P,
    /// Incoming requests
    incoming: Receiver<PersistenceAction>,
    /// The pruner
    pruner: PrunerWithFactory<ProviderFactory<N>>,
    /// metrics
    metrics: PersistenceMetrics,
    /// Sender for sync metrics - we only submit sync metrics for persisted blocks
    sync_metrics_tx: MetricEventsSender,
    /// Flag indicating whether to enable the state cache for persisted blocks
    enable_state_cache: bool,
}

impl<N: ProviderNodeTypes, P> PersistenceService<N, P>
where
    P: DatabaseProviderFactory + BlockReader + StateProviderFactory + StateReader + Clone + 'static,
    <P as DatabaseProviderFactory>::Provider: BlockReader,
{
    /// Create a new persistence service
    pub fn new(
        provider: ProviderFactory<N>,
        view_provider: P,
        incoming: Receiver<PersistenceAction>,
        pruner: PrunerWithFactory<ProviderFactory<N>>,
        sync_metrics_tx: MetricEventsSender,
        enable_state_cache: bool,
    ) -> Self {
        Self {
            provider,
            view_provider,
            incoming,
            pruner,
            metrics: PersistenceMetrics::default(),
            sync_metrics_tx,
            enable_state_cache,
        }
    }

    /// Prunes block data before the given block hash according to the configured prune
    /// configuration.
    fn prune_before(&mut self, block_num: u64) -> Result<PrunerOutput, PrunerError> {
        debug!(target: "engine::persistence", ?block_num, "Running pruner");
        let start_time = Instant::now();
        // TODO: doing this properly depends on pruner segment changes
        let result = self.pruner.run(block_num);
        self.metrics.prune_before_duration_seconds.record(start_time.elapsed());
        result
    }
}

impl<N: ProviderNodeTypes, P> PersistenceService<N, P>
where
    P: DatabaseProviderFactory + BlockReader + StateProviderFactory + StateReader + Clone + 'static,
    <P as DatabaseProviderFactory>::Provider: BlockReader,
{
    /// This is the main loop, that will listen to database events and perform the requested
    /// database actions
    pub fn run(mut self) -> Result<(), PersistenceError> {
        // If the receiver errors then senders have disconnected, so the loop should then end.
        while let Ok(action) = self.incoming.recv() {
            match action {
                PersistenceAction::RemoveBlocksAbove(new_tip_num, sender) => {
                    let result = self.on_remove_blocks_above(new_tip_num)?;
                    // send new sync metrics based on removed blocks
                    let _ =
                        self.sync_metrics_tx.send(MetricEvent::SyncHeight { height: new_tip_num });
                    // we ignore the error because the caller may or may not care about the result
                    let _ = sender.send(result);
                }
                PersistenceAction::SaveBlocks(blocks, sender) => {
                    let result = self.on_save_blocks(blocks)?;
                    if let Some(ref num_hash) = result {
                        // send new sync metrics based on saved blocks
                        let _ = self
                            .sync_metrics_tx
                            .send(MetricEvent::SyncHeight { height: num_hash.number });
                    }
                    // we ignore the error because the caller may or may not care about the result
                    let _ = sender.send(result);
                }
                PersistenceAction::SaveBlocksWithStateRootCalculation(
                    blocks,
                    parent_hash,
                    sender,
                ) => {
                    let result =
                        self.on_save_block_with_state_root_calculation(blocks, parent_hash)?;

                    if let Some(ref num_hash) = result.0 {
                        // send new sync metrics based on saved blocks
                        let _ = self
                            .sync_metrics_tx
                            .send(MetricEvent::SyncHeight { height: num_hash.number });
                    }

                    let _ = sender.send(result);
                }
                PersistenceAction::PruneBefore(block_num, sender) => {
                    let res = self.prune_before(block_num)?;

                    // we ignore the error because the caller may or may not care about the result
                    let _ = sender.send(res);
                }
                PersistenceAction::SaveFinalizedBlock(finalized_block) => {
                    let provider = self.provider.database_provider_rw()?;
                    provider.save_finalized_block_number(finalized_block)?;
                    provider.commit()?;
                }
                PersistenceAction::SaveSafeBlock(safe_block) => {
                    let provider = self.provider.database_provider_rw()?;
                    provider.save_safe_block_number(safe_block)?;
                    provider.commit()?;
                }
            }
        }
        Ok(())
    }

    fn on_remove_blocks_above(
        &self,
        new_tip_num: u64,
    ) -> Result<Option<BlockNumHash>, PersistenceError> {
        debug!(target: "engine::persistence", ?new_tip_num, "Removing blocks");
        let start_time = Instant::now();
        let provider_rw = self.provider.database_provider_rw()?;
        let sf_provider = self.provider.static_file_provider();

        let new_tip_hash = provider_rw.block_hash(new_tip_num)?;
        UnifiedStorageWriter::from(&provider_rw, &sf_provider).remove_blocks_above(new_tip_num)?;
        UnifiedStorageWriter::commit_unwind(provider_rw, sf_provider)?;

        if self.enable_state_cache {
            reth_chain_state::cache::clear_cache();
            debug!(target: "tree::persistence", "Finish to clear state cache");
        }

        debug!(target: "engine::persistence", ?new_tip_num, ?new_tip_hash, "Removed blocks from disk");
        self.metrics.remove_blocks_above_duration_seconds.record(start_time.elapsed());
        Ok(new_tip_hash.map(|hash| BlockNumHash { hash, number: new_tip_num }))
    }

    fn on_save_blocks(
        &self,
        blocks: Vec<ExecutedBlock>,
    ) -> Result<Option<BlockNumHash>, PersistenceError> {
        debug!(target: "engine::persistence", first=?blocks.first().map(|b| b.block.num_hash()), last=?blocks.last().map(|b| b.block.num_hash()), "Saving range of blocks");
        let start_time = Instant::now();
        let last_block_hash_num = blocks
            .last()
            .map(|block| BlockNumHash { hash: block.block().hash(), number: block.block().number });

        if last_block_hash_num.is_some() {
            if self.enable_state_cache {
                // update plain state cache
                reth_chain_state::cache::write_to_cache(blocks.clone());
                debug!(target: "tree::persistence", "Finish to write state cache");
            }

            let provider_rw = self.provider.database_provider_rw()?;
            let static_file_provider = self.provider.static_file_provider();

            UnifiedStorageWriter::from(&provider_rw, &static_file_provider).save_blocks(&blocks)?;
            UnifiedStorageWriter::commit(provider_rw, static_file_provider)?;
        }
        self.metrics.save_blocks_duration_seconds.record(start_time.elapsed());
        Ok(last_block_hash_num)
    }

    fn on_save_block_with_state_root_calculation(
        &self,
        mut blocks: Vec<ExecutedBlock>,
        parent_hash: B256,
    ) -> Result<(Option<BlockNumHash>, TrieUpdates), PersistenceError> {
        debug!(target: "engine::persistence", first=?blocks.first().map(|b| b.block.num_hash()), last=?blocks.last().map(|b| b.block.num_hash()), "Saving range of blocks");

        let state_root_result = self
            .compute_state_root_in_batch_blocks(blocks.clone(), parent_hash)
            .map_err(PersistenceError::StateRootError)?;

        if let Some(last_block) = blocks.last_mut() {
            last_block.set_trie_updates(state_root_result.1.clone());
        }

        let save_blocks_result = self.on_save_blocks(blocks)?;
        Ok((save_blocks_result, state_root_result.1))
    }

    fn compute_state_root_in_batch_blocks(
        &self,
        blocks: Vec<ExecutedBlock>,
        parent_hash: B256,
    ) -> Result<(B256, TrieUpdates), AdvanceCalculateStateRootError> {
        let mut hashed_state = HashedPostState::default();
        for block in &blocks {
            hashed_state.extend(block.hashed_state().clone());
        }
        let block_number = blocks.last().unwrap().block().number;
        let block_hash = blocks.last().unwrap().block().hash();
        let target_state_root = blocks.last().unwrap().state_root();

        let root_time = Instant::now();
        debug!(target: "engine::persistence", ?block_number, ?block_hash, "Computing state root");
        let state_root_result = match self.compute_state_root_parallel(parent_hash, &hashed_state) {
            Ok((state_root, trie_output)) => Some((state_root, trie_output)),
            Err(ParallelStateRootError::Provider(ProviderError::ConsistentView(error))) => {
                debug!(target: "engine::persistence", %error, "Parallel state root computation failed consistency check, falling back");
                None
            }
            Err(error) => return Err(AdvanceCalculateStateRootError::ComputeFailed(error)),
        };

        let (state_root, trie_output) = if let Some(result) = state_root_result {
            result
        } else {
            return Err(AdvanceCalculateStateRootError::ResultNotFound());
        };

        let root_elapsed = root_time.elapsed();
        debug!(target: "engine::persistence", ?block_number, ?block_hash, ?state_root, ?root_elapsed, "Computed state root");

        if state_root != target_state_root {
            return Err(AdvanceCalculateStateRootError::StateRootDiff(GotExpected {
                got: state_root,
                expected: target_state_root,
            }))
        }

        Ok((state_root, trie_output))
    }

    fn compute_state_root_parallel(
        &self,
        parent_hash: B256,
        hashed_state: &HashedPostState,
    ) -> Result<(B256, TrieUpdates), ParallelStateRootError> {
        let consistent_view = ConsistentDbView::new_with_latest_tip(self.view_provider.clone())?;
        let mut input = TrieInput::default();

        let revert_state = consistent_view.revert_state(parent_hash)?;
        input.append(revert_state);

        // Extend with block we are validating root for.
        input.append_ref(hashed_state);

        ParallelStateRoot::new(consistent_view, input).incremental_root_with_updates()
    }
}

/// One of the errors that can happen when using the persistence service.
#[derive(Debug, Error)]
pub enum PersistenceError {
    /// A pruner error
    #[error(transparent)]
    PrunerError(#[from] PrunerError),

    /// A provider error
    #[error(transparent)]
    ProviderError(#[from] ProviderError),

    /// A state root error
    #[error(transparent)]
    StateRootError(#[from] AdvanceCalculateStateRootError),
}

/// This is an error that can come from advancing state root calculation. Either this can be a
/// [`ProviderError`], or this can be a [`GotExpected`]
#[derive(Debug, Error)]
pub enum AdvanceCalculateStateRootError {
    /// A provider error
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// An error that can come from a state root diff
    #[error(transparent)]
    StateRootDiff(#[from] GotExpected<B256>),
    /// An error that can come from a parallel state root error
    #[error(transparent)]
    ComputeFailed(#[from] ParallelStateRootError),
    /// An error that can come from a trie update
    #[error("Result not found")]
    ResultNotFound(),
}

/// A signal to the persistence service that part of the tree state can be persisted.
#[derive(Debug)]
pub enum PersistenceAction {
    /// The section of tree state that should be persisted. These blocks are expected in order of
    /// increasing block number.
    ///
    /// First, header, transaction, and receipt-related data should be written to static files.
    /// Then the execution history-related data will be written to the database.
    SaveBlocks(Vec<ExecutedBlock>, oneshot::Sender<Option<BlockNumHash>>),

    /// The section of tree state that should be persisted. These blocks are expected in order of
    /// increasing block number.
    /// This action will also calculate the state root for the given blocks.
    SaveBlocksWithStateRootCalculation(
        Vec<ExecutedBlock>,
        B256,
        oneshot::Sender<(Option<BlockNumHash>, TrieUpdates)>,
    ),

    /// Removes block data above the given block number from the database.
    ///
    /// This will first update checkpoints from the database, then remove actual block data from
    /// static files.
    RemoveBlocksAbove(u64, oneshot::Sender<Option<BlockNumHash>>),

    /// Prune associated block data before the given block number, according to already-configured
    /// prune modes.
    PruneBefore(u64, oneshot::Sender<PrunerOutput>),

    /// Update the persisted finalized block on disk
    SaveFinalizedBlock(u64),

    /// Update the persisted safe block on disk
    SaveSafeBlock(u64),
}

/// A handle to the persistence service
#[derive(Debug, Clone)]
pub struct PersistenceHandle {
    /// The channel used to communicate with the persistence service
    sender: Sender<PersistenceAction>,
}

impl PersistenceHandle {
    /// Create a new [`PersistenceHandle`] from a [`Sender<PersistenceAction>`].
    pub const fn new(sender: Sender<PersistenceAction>) -> Self {
        Self { sender }
    }

    /// Create a new [`PersistenceHandle`], and spawn the persistence service.
    pub fn spawn_service<N: ProviderNodeTypes, P>(
        provider_factory: ProviderFactory<N>,
        view_provider: P,
        pruner: PrunerWithFactory<ProviderFactory<N>>,
        sync_metrics_tx: MetricEventsSender,
        enable_state_cache: bool,
    ) -> Self
    where
        P: DatabaseProviderFactory
            + BlockReader
            + StateProviderFactory
            + StateReader
            + Clone
            + 'static,
        <P as DatabaseProviderFactory>::Provider: BlockReader,
    {
        // create the initial channels
        let (db_service_tx, db_service_rx) = std::sync::mpsc::channel();

        // construct persistence handle
        let persistence_handle = Self::new(db_service_tx);

        // spawn the persistence service
        let db_service = PersistenceService::new(
            provider_factory,
            view_provider,
            db_service_rx,
            pruner,
            sync_metrics_tx,
            enable_state_cache,
        );
        std::thread::Builder::new()
            .name("Persistence Service".to_string())
            .spawn(|| {
                if let Err(err) = db_service.run() {
                    error!(target: "engine::persistence", ?err, "Persistence service failed");
                }
            })
            .unwrap();

        persistence_handle
    }

    /// Sends a specific [`PersistenceAction`] in the contained channel. The caller is responsible
    /// for creating any channels for the given action.
    pub fn send_action(
        &self,
        action: PersistenceAction,
    ) -> Result<(), SendError<PersistenceAction>> {
        self.sender.send(action)
    }

    /// Tells the persistence service to save a certain list of finalized blocks. The blocks are
    /// assumed to be ordered by block number.
    ///
    /// This returns the latest hash that has been saved, allowing removal of that block and any
    /// previous blocks from in-memory data structures. This value is returned in the receiver end
    /// of the sender argument.
    ///
    /// If there are no blocks to persist, then `None` is sent in the sender.
    pub fn save_blocks(
        &self,
        blocks: Vec<ExecutedBlock>,
        tx: oneshot::Sender<Option<BlockNumHash>>,
    ) -> Result<(), SendError<PersistenceAction>> {
        self.send_action(PersistenceAction::SaveBlocks(blocks, tx))
    }

    /// Persists the finalized block number on disk.
    /// This will also calculate the state root for the given blocks.
    /// The resulting [`TrieUpdates`] is returned in the receiver end of the sender argument.
    /// The new tip hash is returned in the receiver end of the sender argument.
    pub fn save_blocks_with_state_root_calculation(
        &self,
        blocks: Vec<ExecutedBlock>,
        parent_hash: B256,
        tx: oneshot::Sender<(Option<BlockNumHash>, TrieUpdates)>,
    ) -> Result<(), SendError<PersistenceAction>> {
        self.send_action(PersistenceAction::SaveBlocksWithStateRootCalculation(
            blocks,
            parent_hash,
            tx,
        ))
    }

    /// Persists the finalized block number on disk.
    pub fn save_finalized_block_number(
        &self,
        finalized_block: u64,
    ) -> Result<(), SendError<PersistenceAction>> {
        self.send_action(PersistenceAction::SaveFinalizedBlock(finalized_block))
    }

    /// Persists the finalized block number on disk.
    pub fn save_safe_block_number(
        &self,
        safe_block: u64,
    ) -> Result<(), SendError<PersistenceAction>> {
        self.send_action(PersistenceAction::SaveSafeBlock(safe_block))
    }

    /// Tells the persistence service to remove blocks above a certain block number. The removed
    /// blocks are returned by the service.
    ///
    /// When the operation completes, the new tip hash is returned in the receiver end of the sender
    /// argument.
    pub fn remove_blocks_above(
        &self,
        block_num: u64,
        tx: oneshot::Sender<Option<BlockNumHash>>,
    ) -> Result<(), SendError<PersistenceAction>> {
        self.send_action(PersistenceAction::RemoveBlocksAbove(block_num, tx))
    }

    /// Tells the persistence service to remove block data before the given hash, according to the
    /// configured prune config.
    ///
    /// The resulting [`PrunerOutput`] is returned in the receiver end of the sender argument.
    pub fn prune_before(
        &self,
        block_num: u64,
        tx: oneshot::Sender<PrunerOutput>,
    ) -> Result<(), SendError<PersistenceAction>> {
        self.send_action(PersistenceAction::PruneBefore(block_num, tx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use reth_chain_state::test_utils::TestBlockBuilder;
    use reth_exex_types::FinishedExExHeight;
    use reth_provider::test_utils::create_test_provider_factory;
    use reth_prune::Pruner;
    use tokio::sync::mpsc::unbounded_channel;

    fn default_persistence_handle() -> PersistenceHandle {
        let provider = create_test_provider_factory();

        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);

        let pruner = Pruner::new_with_factory(
            provider.clone(),
            vec![],
            5,
            0,
            None,
            finished_exex_height_rx,
            0,
            Some(provider.static_file_provider().path().to_path_buf()),
        );

        let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
        PersistenceHandle::spawn_service(provider, pruner, sync_metrics_tx, false)
    }

    #[tokio::test]
    async fn test_save_blocks_empty() {
        reth_tracing::init_test_tracing();
        let persistence_handle = default_persistence_handle();

        let blocks = vec![];
        let (tx, rx) = oneshot::channel();

        persistence_handle.save_blocks(blocks, tx).unwrap();

        let hash = rx.await.unwrap();
        assert_eq!(hash, None);
    }

    #[tokio::test]
    async fn test_save_blocks_single_block() {
        reth_tracing::init_test_tracing();
        let persistence_handle = default_persistence_handle();
        let block_number = 0;
        let mut test_block_builder = TestBlockBuilder::default();
        let executed =
            test_block_builder.get_executed_block_with_number(block_number, B256::random());
        let block_hash = executed.block().hash();

        let blocks = vec![executed];
        let (tx, rx) = oneshot::channel();

        persistence_handle.save_blocks(blocks, tx).unwrap();

        let BlockNumHash { hash: actual_hash, number: _ } =
            tokio::time::timeout(std::time::Duration::from_secs(10), rx)
                .await
                .expect("test timed out")
                .expect("channel closed unexpectedly")
                .expect("no hash returned");

        assert_eq!(block_hash, actual_hash);
    }

    #[tokio::test]
    async fn test_save_blocks_multiple_blocks() {
        reth_tracing::init_test_tracing();
        let persistence_handle = default_persistence_handle();

        let mut test_block_builder = TestBlockBuilder::default();
        let blocks = test_block_builder.get_executed_blocks(0..5).collect::<Vec<_>>();
        let last_hash = blocks.last().unwrap().block().hash();
        let (tx, rx) = oneshot::channel();

        persistence_handle.save_blocks(blocks, tx).unwrap();
        let BlockNumHash { hash: actual_hash, number: _ } = rx.await.unwrap().unwrap();
        assert_eq!(last_hash, actual_hash);
    }

    #[tokio::test]
    async fn test_save_blocks_multiple_calls() {
        reth_tracing::init_test_tracing();
        let persistence_handle = default_persistence_handle();

        let ranges = [0..1, 1..2, 2..4, 4..5];
        let mut test_block_builder = TestBlockBuilder::default();
        for range in ranges {
            let blocks = test_block_builder.get_executed_blocks(range).collect::<Vec<_>>();
            let last_hash = blocks.last().unwrap().block().hash();
            let (tx, rx) = oneshot::channel();

            persistence_handle.save_blocks(blocks, tx).unwrap();

            let BlockNumHash { hash: actual_hash, number: _ } = rx.await.unwrap().unwrap();
            assert_eq!(last_hash, actual_hash);
        }
    }
}
