//! EVM config for bsc.

// TODO: doc
#![allow(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
// The `bsc` feature must be enabled to use this crate.
#![cfg(feature = "bsc")]

use std::{convert::Infallible, sync::Arc};

use alloy_primitives::{Address, Bytes, U256};
use reth_bsc_chainspec::BscChainSpec;
use reth_ethereum_forks::EthereumHardfork;
use reth_evm::{ConfigureEvm, ConfigureEvmEnv, NextBlockEnvAttributes};
use reth_primitives::{
    constants::EIP1559_INITIAL_BASE_FEE,
    revm_primitives::{
        AnalysisKind, BlobExcessGasAndPrice, BlockEnv, CfgEnv, CfgEnvWithHandlerCfg, SpecId, TxEnv,
    },
    transaction::FillTxEnv,
    Head, Header, TransactionSigned,
};
use reth_revm::{inspector_handle_register, Database, Evm, EvmBuilder, GetInspector};
use revm_primitives::Env;

mod config;
pub use config::{revm_spec, revm_spec_by_timestamp_after_shanghai};
mod execute;
pub use execute::*;
mod error;
pub use error::BscBlockExecutionError;
mod patch_hertz;
mod post_execution;
mod pre_execution;

/// Bsc-related EVM configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BscEvmConfig {
    chain_spec: Arc<BscChainSpec>,
}

impl BscEvmConfig {
    /// Creates a new Ethereum EVM configuration with the given chain spec.
    pub const fn new(chain_spec: Arc<BscChainSpec>) -> Self {
        Self { chain_spec }
    }

    /// Returns the chain spec associated with this configuration.
    pub fn chain_spec(&self) -> &BscChainSpec {
        &self.chain_spec
    }
}

impl ConfigureEvmEnv for BscEvmConfig {
    type Header = Header;
    type Error = Infallible; // TODO: error type

    fn fill_tx_env(&self, tx_env: &mut TxEnv, transaction: &TransactionSigned, sender: Address) {
        transaction.fill_tx_env(tx_env, sender);
    }

    fn fill_tx_env_system_contract_call(
        &self,
        _env: &mut Env,
        _caller: Address,
        _contract: Address,
        _data: Bytes,
    ) {
        // No system contract call on BSC
    }

    fn fill_cfg_env(
        &self,
        cfg_env: &mut CfgEnvWithHandlerCfg,
        header: &Header,
        total_difficulty: U256,
    ) {
        let spec_id = revm_spec(
            self.chain_spec(),
            &Head {
                number: header.number,
                timestamp: header.timestamp,
                difficulty: header.difficulty,
                total_difficulty,
                hash: Default::default(),
            },
        );

        cfg_env.chain_id = self.chain_spec.chain().id();
        cfg_env.perf_analyse_created_bytecodes = AnalysisKind::Analyse;

        // Disable block gas limit check
        // system transactions do not have gas limit
        cfg_env.disable_block_gas_limit = true;

        cfg_env.handler_cfg.spec_id = spec_id;
        cfg_env.handler_cfg.is_bsc = self.chain_spec.is_bsc();
    }

    fn next_cfg_and_block_env(
        &self,
        parent: &Self::Header,
        attributes: NextBlockEnvAttributes,
    ) -> Result<(CfgEnvWithHandlerCfg, BlockEnv), Self::Error> {
        // configure evm env based on parent block
        let cfg = CfgEnv::default().with_chain_id(self.chain_spec.chain().id());

        // ensure we're not missing any timestamp based hardforks
        let spec_id = revm_spec_by_timestamp_after_shanghai(&self.chain_spec, attributes.timestamp);

        // if the parent block did not have excess blob gas (i.e. it was pre-cancun), but it is
        // cancun now, we need to set the excess blob gas to the default value
        let blob_excess_gas_and_price = parent
            .next_block_excess_blob_gas()
            .or_else(|| (spec_id == SpecId::CANCUN).then_some(0))
            .map(BlobExcessGasAndPrice::new);

        let mut basefee = parent.next_block_base_fee(
            self.chain_spec.base_fee_params_at_timestamp(attributes.timestamp),
        );

        let mut gas_limit = U256::from(parent.gas_limit);

        // If we are on the London fork boundary, we need to multiply the parent's gas limit by the
        // elasticity multiplier to get the new gas limit.
        if self.chain_spec.fork(EthereumHardfork::London).transitions_at_block(parent.number + 1) {
            let elasticity_multiplier = self
                .chain_spec
                .base_fee_params_at_timestamp(attributes.timestamp)
                .elasticity_multiplier;

            // multiply the gas limit by the elasticity multiplier
            gas_limit *= U256::from(elasticity_multiplier);

            // set the base fee to the initial base fee from the EIP-1559 spec
            basefee = Some(EIP1559_INITIAL_BASE_FEE)
        }

        let block_env = BlockEnv {
            number: U256::from(parent.number + 1),
            coinbase: attributes.suggested_fee_recipient,
            timestamp: U256::from(attributes.timestamp),
            difficulty: U256::ZERO,
            prevrandao: Some(attributes.prev_randao),
            gas_limit,
            // calculate basefee based on parent block's gas usage
            basefee: basefee.map(U256::from).unwrap_or_default(),
            // calculate excess gas based on parent block's blob gas usage
            blob_excess_gas_and_price,
        };

        Ok((CfgEnvWithHandlerCfg::new_with_spec_id(cfg, spec_id), block_env))
    }
}

impl ConfigureEvm for BscEvmConfig {
    type DefaultExternalContext<'a> = ();

    fn evm<DB: Database>(&self, db: DB) -> Evm<'_, Self::DefaultExternalContext<'_>, DB> {
        EvmBuilder::default().with_db(db).bsc().build()
    }

    fn evm_with_inspector<DB, I>(&self, db: DB, inspector: I) -> Evm<'_, I, DB>
    where
        DB: Database,
        I: GetInspector<DB>,
    {
        EvmBuilder::default()
            .with_db(db)
            .with_external_context(inspector)
            .bsc()
            .append_handler_register(inspector_handle_register)
            .build()
    }

    fn default_external_context<'a>(&self) -> Self::DefaultExternalContext<'a> {}
}

#[cfg(test)]
mod tests {
    use alloy_genesis::Genesis;
    use reth_chainspec::{Chain, ChainSpec};
    use reth_primitives::revm_primitives::{BlockEnv, CfgEnv};
    use revm_primitives::SpecId;

    use super::*;

    #[test]
    #[ignore]
    fn test_fill_cfg_and_block_env() {
        let mut cfg_env = CfgEnvWithHandlerCfg::new_with_spec_id(CfgEnv::default(), SpecId::LATEST);
        let mut block_env = BlockEnv::default();
        let header = Header::default();
        let total_difficulty = U256::ZERO;

        let chain_spec = ChainSpec::builder()
            .chain(Chain::bsc_mainnet())
            .genesis(Genesis::default())
            .london_activated()
            .paris_activated()
            .shanghai_activated()
            .build();

        BscEvmConfig::new(Arc::new(BscChainSpec { inner: chain_spec.clone() }))
            .fill_cfg_and_block_env(&mut cfg_env, &mut block_env, &header, total_difficulty);

        assert_eq!(cfg_env.chain_id, chain_spec.chain().id());
    }
}
