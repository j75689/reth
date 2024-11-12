use std::sync::Arc;

use alloy_primitives::{address, map::HashMap, Address, U256};
use reth_bsc_forks::BscHardforks;
use reth_bsc_primitives::system_contracts::get_upgrade_system_contracts;
use reth_primitives::revm_primitives::{
    db::{Database, DatabaseCommit},
    state::AccountStatus,
    BlockEnv,
};
use reth_revm::primitives::Account;

use crate::Parlia;

// redefine to avoid dependency on revm/bsc
const SYSTEM_ADDRESS: Address = address!("fffffffffffffffffffffffffffffffffffffffe");

#[derive(Debug, Clone)]
pub struct BscTraceHelper {
    parlia: Arc<Parlia>,
}

impl BscTraceHelper {
    pub const fn new(parlia: Arc<Parlia>) -> Self {
        Self { parlia }
    }

    pub fn upgrade_system_contracts<DB: Database + DatabaseCommit>(
        &self,
        db: &mut DB,
        block_env: &BlockEnv,
        parent_timestamp: u64,
        before_tx: bool,
    ) -> Result<(), BscTraceHelperError> {
        let is_feynman_active =
            self.parlia.chain_spec().is_feynman_active_at_timestamp(block_env.timestamp.to());

        if (before_tx && !is_feynman_active) || (!before_tx && is_feynman_active) {
            let contracts = get_upgrade_system_contracts(
                self.parlia.chain_spec(),
                block_env.number.to(),
                block_env.timestamp.to(),
                parent_timestamp,
            )
            .map_err(|_| BscTraceHelperError::GetUpgradeSystemContractsFailed)?;

            let mut changeset: HashMap<_, _> = Default::default();
            for (k, v) in contracts {
                let mut info = db
                    .basic(k)
                    .map_err(|_| BscTraceHelperError::LoadAccountFailed)?
                    .unwrap_or_default()
                    .clone();

                info.code_hash = v.clone().unwrap().hash_slow();
                info.code = v;

                let mut account = Account::default();
                account.info = info;
                account.status = AccountStatus::Touched;

                changeset.insert(k, account);
            }

            db.commit(changeset);
        }

        Ok(())
    }

    pub fn add_block_reward<DB: Database + DatabaseCommit>(
        &self,
        db: &mut DB,
        block_env: &BlockEnv,
    ) -> Result<(), BscTraceHelperError> {
        let mut sys_info = db
            .basic(SYSTEM_ADDRESS)
            .map_err(|_| BscTraceHelperError::LoadAccountFailed)?
            .unwrap_or_default()
            .clone();
        let balance = sys_info.balance;
        if balance > U256::ZERO {
            let mut changeset: HashMap<_, _> = Default::default();

            sys_info.balance = U256::ZERO;

            let mut sys_account = Account::default();
            sys_account.info = sys_info;
            sys_account.status = AccountStatus::Touched;
            changeset.insert(SYSTEM_ADDRESS, sys_account);

            let mut val_info = db
                .basic(block_env.coinbase)
                .map_err(|_| BscTraceHelperError::LoadAccountFailed)?
                .unwrap_or_default()
                .clone();

            val_info.balance += balance;

            let mut val_account = Account::default();
            val_account.info = val_info;
            val_account.status = AccountStatus::Touched;
            changeset.insert(block_env.coinbase, val_account);

            db.commit(changeset);
        }

        Ok(())
    }
}

/// Errors that can occur when calling `BscTraceHelper` methods
#[derive(Debug, thiserror::Error)]
pub enum BscTraceHelperError {
    #[error("Failed to load account from db")]
    LoadAccountFailed,
    #[error("Failed to get upgrade system contracts")]
    GetUpgradeSystemContractsFailed,
}
