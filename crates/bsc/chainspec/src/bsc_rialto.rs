//! Chain specification for the BSC Mainnet network.

#[cfg(not(feature = "std"))]
use alloc::sync::Arc;
#[cfg(feature = "std")]
use std::sync::Arc;

use alloy_chains::Chain;
use alloy_primitives::{b256, U256};
use once_cell::sync::Lazy;
use reth_bsc_forks::BscHardfork;
use reth_chainspec::{once_cell_set, BaseFeeParams, BaseFeeParamsKind, ChainSpec};

use crate::BscChainSpec;

/// The BSC mainnet spec
pub static BSC_RIALTO: Lazy<Arc<BscChainSpec>> = Lazy::new(|| {
    BscChainSpec {
        inner: ChainSpec {
            chain: Chain::from_id(714),
            genesis: serde_json::from_str(include_str!("../res/genesis/bsc_rialto.json"))
                .expect("Can't deserialize BSC Rialto genesis json"),
            genesis_hash: once_cell_set(b256!(
                "f58dd0f71313a12b473b50988ac7dd06cc9664aad3c8d7d3716ffaf680e305b4"
            )),
            paris_block_and_final_difficulty: Some((0, U256::from(0))),
            hardforks: BscHardfork::bsc_qa(),
            deposit_contract: None,
            base_fee_params: BaseFeeParamsKind::Constant(BaseFeeParams::new(1, 1)),
            prune_delete_limit: 3500,
            ..Default::default()
        },
    }
    .into()
});
