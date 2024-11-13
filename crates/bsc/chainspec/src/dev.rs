//! Chain specification in dev mode for custom chain.

#[cfg(not(feature = "std"))]
use alloc::sync::Arc;
#[cfg(feature = "std")]
use std::sync::Arc;

use alloy_chains::Chain;
use alloy_consensus::constants::DEV_GENESIS_HASH;
use alloy_primitives::U256;
use once_cell::sync::Lazy;
use reth_bsc_forks::DEV_HARDFORKS;
use reth_chainspec::{once_cell_set, BaseFeeParams, BaseFeeParamsKind, ChainSpec};

use crate::BscChainSpec;

/// Bsc dev testnet specification
///
/// Includes 20 prefunded accounts with `10_000` ETH each derived from mnemonic "test test test test
/// test test test test test test test junk".
pub static BSC_DEV: Lazy<Arc<BscChainSpec>> = Lazy::new(|| {
    {
        BscChainSpec {
            inner: ChainSpec {
                chain: Chain::dev(),
                genesis: serde_json::from_str(include_str!("../res/genesis/dev.json"))
                    .expect("Can't deserialize Dev testnet genesis json"),
                genesis_hash: once_cell_set(DEV_GENESIS_HASH),
                paris_block_and_final_difficulty: Some((0, U256::from(0))),
                hardforks: DEV_HARDFORKS.clone(),
                deposit_contract: None,
                base_fee_params: BaseFeeParamsKind::Constant(BaseFeeParams::new(1, 1)),
                prune_delete_limit: 3500,
                ..Default::default()
            },
        }
    }
    .into()
});
