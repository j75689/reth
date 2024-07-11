use crate::{client::ParliaClient, Storage};
use alloy_rlp::Encodable;
use reth_beacon_consensus::{BeaconEngineMessage, ForkchoiceStatus};
use reth_bsc_consensus::Parlia;
use reth_chainspec::ChainSpec;
use reth_engine_primitives::EngineTypes;
use reth_evm_bsc::SnapshotReader;
use reth_network::message::EngineMessage;
use reth_network_p2p::{headers::client::HeadersClient, priority::Priority};
use reth_primitives::{Block, BlockBody, BlockHashOrNumber, SealedHeader};
use reth_provider::{BlockReaderIdExt, CanonChainTracker, ParliaProvider};
use reth_rpc_types::engine::ForkchoiceState;
use std::{
    clone::Clone,
    fmt,
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

use reth_network_p2p::bodies::client::BodiesClient;
use tokio::{
    signal,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::{interval, sleep, timeout, Duration},
};
use tracing::{debug, error, info, trace};

/// All message variants that can be sent to beacon engine.
#[derive(Debug)]
enum ForkChoiceMessage {
    /// Broadcast new hash.
    NewHeader(NewHeaderEvent),
}
/// internal message to beacon engine
#[derive(Debug, Clone)]
struct NewHeaderEvent {
    header: SealedHeader,
}

/// A struct that contains a block hash or number and a block
#[derive(Debug, Clone)]
struct BlockInfo {
    block_hash: BlockHashOrNumber,
    block_number: u64,
    block: Option<Block>,
}

/// A Future that listens for new headers and puts into storage
pub(crate) struct ParliaEngineTask<
    Engine: EngineTypes,
    Provider: BlockReaderIdExt + CanonChainTracker,
    P: ParliaProvider,
> {
    /// The configured chain spec
    chain_spec: Arc<ChainSpec>,
    /// The coneensus instance
    consensus: Parlia,
    /// The provider used to read the block and header from the inserted chain
    provider: Provider,
    /// The snapshot reader used to read the snapshot
    snapshot_reader: Arc<SnapshotReader<P>>,
    /// The client used to fetch headers
    block_fetcher: ParliaClient,
    /// The interval of the block producing
    block_interval: u64,
    /// Shared storage to insert new headers
    storage: Storage,
    /// The engine to send messages to the beacon engine
    to_engine: UnboundedSender<BeaconEngineMessage<Engine>>,
    /// The watch for the network block event receiver
    network_block_event_rx: Arc<Mutex<UnboundedReceiver<EngineMessage>>>,
    /// The channel to send fork choice messages
    fork_choice_tx: UnboundedSender<ForkChoiceMessage>,
    /// The channel to receive fork choice messages
    fork_choice_rx: Arc<Mutex<UnboundedReceiver<ForkChoiceMessage>>>,
    /// The channel to send chain tracker messages
    chain_tracker_tx: UnboundedSender<ForkChoiceMessage>,
    /// The channel to receive chain tracker messages
    chain_tracker_rx: Arc<Mutex<UnboundedReceiver<ForkChoiceMessage>>>,
    /// The flag to indicate if the fork choice update is syncing
    syncing_fcu: Arc<AtomicBool>,
}

// === impl ParliaEngineTask ===
impl<
        Engine: EngineTypes + 'static,
        Provider: BlockReaderIdExt + CanonChainTracker + Clone + 'static,
        P: ParliaProvider + 'static,
    > ParliaEngineTask<Engine, Provider, P>
{
    /// Creates a new instance of the task
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        chain_spec: Arc<ChainSpec>,
        consensus: Parlia,
        provider: Provider,
        snapshot_reader: SnapshotReader<P>,
        to_engine: UnboundedSender<BeaconEngineMessage<Engine>>,
        network_block_event_rx: Arc<Mutex<UnboundedReceiver<EngineMessage>>>,
        storage: Storage,
        block_fetcher: ParliaClient,
        block_interval: u64,
    ) {
        let (fork_choice_tx, fork_choice_rx) = mpsc::unbounded_channel();
        let (chain_tracker_tx, chain_tracker_rx) = mpsc::unbounded_channel();
        let this = Self {
            chain_spec,
            consensus,
            provider,
            snapshot_reader: Arc::new(snapshot_reader),
            to_engine,
            network_block_event_rx,
            storage,
            block_fetcher,
            block_interval,
            fork_choice_tx,
            fork_choice_rx: Arc::new(Mutex::new(fork_choice_rx)),
            chain_tracker_tx,
            chain_tracker_rx: Arc::new(Mutex::new(chain_tracker_rx)),
            syncing_fcu: Arc::new(AtomicBool::new(false)),
        };

        this.start_block_event_listening();
        this.start_fork_choice_update_notifier();
        this.start_chain_tracker_notifier();
    }

    /// Start listening to the network block event
    fn start_block_event_listening(&self) {
        let engine_rx = self.network_block_event_rx.clone();
        let block_interval = self.block_interval;
        let mut interval = interval(Duration::from_secs(block_interval));
        let chain_spec = self.chain_spec.clone();
        let storage = self.storage.clone();
        let client = self.provider.clone();
        let block_fetcher = self.block_fetcher.clone();
        let consensus = self.consensus.clone();
        let fork_choice_tx = self.fork_choice_tx.clone();
        let fetch_header_timeout_duration = Duration::from_secs(block_interval);
        let syncing_fcu = self.syncing_fcu.clone();

        tokio::spawn(async move {
            loop {
                let read_storage = storage.read().await;
                let best_header = read_storage.best_header.clone();
                drop(read_storage);
                let mut engine_rx_guard = engine_rx.lock().await;
                let mut info = BlockInfo {
                    block_hash: BlockHashOrNumber::from(0),
                    block_number: 0,
                    block: None,
                };
                tokio::select! {
                    msg = engine_rx_guard.recv() => {
                        if msg.is_none() {
                            continue;
                        }
                        match msg.unwrap() {
                            EngineMessage::NewBlockHashes(event) => match event.hashes.last() {
                                None => continue,
                                Some(block_hash) => {
                                    info.block_hash = BlockHashOrNumber::Hash(block_hash.hash);
                                    info.block_number = block_hash.number;
                                }
                            },
                            EngineMessage::NewBlock(event) => {
                                info.block_hash = BlockHashOrNumber::Hash(event.hash);
                                info.block_number = event.block.block.number;
                                info.block = Some(event.block.block.clone());
                            }
                        }
                    }
                    _ = interval.tick() => {
                        // If head has not been updated for a long time, take the initiative to get it
                        if SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs()
                            - best_header.timestamp
                            < 10 || best_header.number == 0
                        {
                            continue;
                        }
                        info.block_hash = BlockHashOrNumber::Number(best_header.number+1);
                        info.block_number = best_header.number+1;
                    }
                    _ = signal::ctrl_c() => {
                        info!(target: "consensus::parlia", "block event listener shutting down...");
                        return
                    },
                }

                let is_syncing_fcu = syncing_fcu.load(std::sync::atomic::Ordering::Relaxed);
                if is_syncing_fcu {
                    continue
                }

                // skip if number is lower than best number
                if info.block_number <= best_header.number {
                    continue;
                }

                let mut header_option = match info.block.clone() {
                    Some(block) => Some(block.header),
                    None => None,
                };

                if header_option.is_none() {
                    debug!(target: "consensus::parlia", { block_hash = ?info.block_hash }, "Fetching new header");
                    // fetch header and verify
                    let fetch_header_result = match timeout(
                        fetch_header_timeout_duration,
                        block_fetcher.get_header_with_priority(info.block_hash, Priority::High),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            trace!(target: "consensus::parlia", "Fetch header timeout");
                            continue
                        }
                    };
                    if fetch_header_result.is_err() {
                        trace!(target: "consensus::parlia", "Failed to fetch header");
                        continue
                    }

                    header_option = fetch_header_result.unwrap().into_data();
                    if header_option.is_none() {
                        trace!(target: "consensus::parlia", "Failed to unwrap header");
                        continue
                    }
                }
                let latest_header = header_option.unwrap();

                // skip if parent hash is not equal to best hash
                if latest_header.number == best_header.number + 1 &&
                    latest_header.parent_hash != best_header.hash()
                {
                    continue;
                }

                let trusted_header = client
                    .latest_header()
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| chain_spec.sealed_genesis_header());

                // verify header and timestamp
                // predict timestamp is the trusted header timestamp plus the block interval times
                // the difference between the latest header number and the trusted
                // header number the timestamp of latest header should be bigger
                // than the predicted timestamp and less than the current timestamp.
                let predicted_timestamp = trusted_header.timestamp +
                    block_interval * (latest_header.number - trusted_header.number);
                let sealed_header = latest_header.clone().seal_slow();
                let is_valid_header = match consensus
                    .validate_header_with_predicted_timestamp(&sealed_header, predicted_timestamp)
                {
                    Ok(_) => true,
                    Err(err) => {
                        debug!(target: "consensus::parlia", %err, "Parlia verify header failed");
                        false
                    }
                };
                trace!(target: "consensus::parlia", sealed_header = ?sealed_header, is_valid_header = ?is_valid_header, "Fetch a sealed header");
                if !is_valid_header {
                    continue
                };

                // cache header and block
                let mut storage = storage.write().await;
                storage.insert_new_header(sealed_header.clone());
                if info.block.is_some() {
                    storage.insert_new_block(
                        sealed_header.clone(),
                        BlockBody::from(info.block.clone().unwrap()),
                    );
                }
                drop(storage);
                let result = fork_choice_tx.send(ForkChoiceMessage::NewHeader(NewHeaderEvent {
                    header: sealed_header.clone(),
                }));
                if result.is_err() {
                    error!(target: "consensus::parlia", "Failed to send new block event to fork choice");
                }
            }
        });
        info!(target: "consensus::parlia", "started listening to network block event")
    }

    fn start_fork_choice_update_notifier(&self) {
        let block_interval = self.block_interval;
        let fork_choice_rx = self.fork_choice_rx.clone();
        let to_engine = self.to_engine.clone();
        let chain_tracker_tx = self.chain_tracker_tx.clone();
        let storage = self.storage.clone();
        let syncing_fcu = self.syncing_fcu.clone();
        tokio::spawn(async move {
            loop {
                let mut fork_choice_rx_guard = fork_choice_rx.lock().await;
                tokio::select! {
                    msg = fork_choice_rx_guard.recv() => {
                        if msg.is_none() {
                            continue;
                        }
                        match msg.unwrap() {
                            ForkChoiceMessage::NewHeader(event) => {
                                // notify parlia engine
                                let new_header = event.header;
                                let storage = storage.read().await;
                                let safe_hash = storage.best_safe_hash;
                                let finalized_hash = storage.best_finalized_hash;
                                drop(storage);

                                let state = ForkchoiceState {
                                    head_block_hash: new_header.hash(),
                                    // safe(justified) and finalized hash will be determined in the parlia consensus engine and stored in the snapshot after the block sync
                                    safe_block_hash: safe_hash,
                                    finalized_block_hash: finalized_hash,
                                };

                                let mut is_valid_fcu = false;
                                syncing_fcu.store(true, std::sync::atomic::Ordering::Relaxed);
                                loop {
                                    // send the new update to the engine, this will trigger the engine
                                    // to download and execute the block we just inserted
                                    let (tx, rx) = oneshot::channel();
                                    let _ = to_engine.send(BeaconEngineMessage::ForkchoiceUpdated {
                                        state,
                                        payload_attrs: None,
                                        tx,
                                    });
                                    debug!(target: "consensus::parlia", ?state, "Sent fork choice update");

                                    let rx_result = match rx.await {
                                        Ok(result) => result,
                                        Err(err)=> {
                                            error!(target: "consensus::parlia", ?err, "Fork choice update response failed");
                                            break
                                        }
                                    };

                                    match rx_result {
                                        Ok(fcu_response) => {
                                            match fcu_response.forkchoice_status() {
                                                ForkchoiceStatus::Valid => {
                                                    trace!(target: "consensus::parlia", ?fcu_response, "Forkchoice update returned valid response");
                                                    is_valid_fcu = true;
                                                    break
                                                }
                                                ForkchoiceStatus::Invalid => {
                                                    error!(target: "consensus::parlia", ?fcu_response, "Forkchoice update returned invalid response");
                                                    break
                                                }
                                                ForkchoiceStatus::Syncing => {
                                                    debug!(target: "consensus::parlia", ?fcu_response, "Forkchoice update returned SYNCING, waiting for VALID");
                                                    sleep(Duration::from_secs(block_interval)).await;
                                                    continue
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            error!(target: "consensus::parlia", %err, "Parlia fork choice update failed");
                                        }
                                    }
                                }
                                syncing_fcu.store(false, std::sync::atomic::Ordering::Relaxed);

                                if !is_valid_fcu {
                                    continue
                                }

                                let result = chain_tracker_tx.send(ForkChoiceMessage::NewHeader(NewHeaderEvent {
                                    header: new_header.clone(),
                                }));
                                if result.is_err() {
                                    error!(target: "consensus::parlia", "Failed to send new block event to chain tracker");
                                }
                            }
                        }
                    }
                    _ = signal::ctrl_c() => {
                        info!(target: "consensus::parlia", "fork choice notifier shutting down...");
                        return
                    },
                }
            }
        });
        info!(target: "consensus::parlia", "started fork choice notifier")
    }

    fn start_chain_tracker_notifier(&self) {
        let chain_tracker_rx = self.chain_tracker_rx.clone();
        let snapshot_reader = self.snapshot_reader.clone();
        let provider = self.provider.clone();
        let storage = self.storage.clone();

        tokio::spawn(async move {
            loop {
                let mut fork_choice_rx_guard = chain_tracker_rx.lock().await;
                tokio::select! {
                    msg = fork_choice_rx_guard.recv() => {
                        if msg.is_none() {
                            continue;
                        }
                        match msg.unwrap() {
                            ForkChoiceMessage::NewHeader(event) => {
                                let new_header = event.header;

                                let snap = match snapshot_reader.snapshot(&new_header, None) {
                                    Ok(snap) => snap,
                                    Err(err) => {
                                        error!(target: "consensus::parlia", %err, "Snapshot not found");
                                        continue
                                    }
                                };
                                // safe finalized and safe hash for next round fcu
                                let finalized_hash = snap.vote_data.source_hash;
                                let safe_hash = snap.vote_data.target_hash;
                                let mut storage = storage.write().await;
                                storage.insert_finalized_and_safe_hash(finalized_hash, safe_hash);
                                drop(storage);

                                // notify chain tracker to help rpc module can know the finalized and safe hash
                                match provider.sealed_header(snap.vote_data.source_number) {
                                    Ok(header) => {
                                        if let Some(sealed_header) = header {
                                            provider.set_finalized(sealed_header.clone());
                                        }
                                    }
                                    Err(err) => {
                                        error!(target: "consensus::parlia", %err, "Failed to get source header");
                                    }
                                }

                                match provider.sealed_header(snap.vote_data.target_number) {
                                    Ok(header) => {
                                        if let Some(sealed_header) = header {
                                            provider.set_safe(sealed_header.clone());
                                        }
                                    }
                                    Err(err) => {
                                        error!(target: "consensus::parlia", %err, "Failed to get target header");
                                    }
                                }
                            }

                        }
                    }
                    _ = signal::ctrl_c() => {
                        info!(target: "consensus::parlia", "chain tracker notifier shutting down...");
                        return
                    },
                }
            }
        });

        info!(target: "consensus::parlia", "started chain tracker notifier")
    }
}

impl<Engine: EngineTypes, Provider: BlockReaderIdExt + CanonChainTracker, P: ParliaProvider>
    fmt::Debug for ParliaEngineTask<Engine, Provider, P>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("chain_spec")
            .field("chain_spec", &self.chain_spec)
            .field("consensus", &self.consensus)
            .field("storage", &self.storage)
            .field("block_fetcher", &self.block_fetcher)
            .field("block_interval", &self.block_interval)
            .finish_non_exhaustive()
    }
}
