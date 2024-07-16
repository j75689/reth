# Changelog

## v1.0.0-rc.2

This release is a release candidate for the v1.0.0 release. It includes a number of new features and bug fixes.

### FEATURES
* [\#49](https://github.com/bnb-chain/reth/pull/49) ci: specify rust toolchain version
* [\#50](https://github.com/bnb-chain/reth/pull/50) chore: add bsc and docker commands to readme
* [\#51](https://github.com/bnb-chain/reth/pull/51) ci: tag non-beta version as latest image
* [\#54](https://github.com/bnb-chain/reth/pull/54) feat: set finalized and safe hash (parlia)
* [\#58](https://github.com/bnb-chain/reth/pull/58) chore: fix system account issue and hertz storage patch issue on testnet
* [\#61](https://github.com/bnb-chain/reth/pull/61) chore: merge with upstream v1.0.1
* [\#67](https://github.com/bnb-chain/reth/pull/67) doc: update README.md

### BUGFIX
* [\#48](https://github.com/bnb-chain/reth/pull/48) fix: disable parlia task if running with debug.tip
* [\#69](https://github.com/bnb-chain/reth/pull/69) fix: add exception for slash system transaction
* [\#70](https://github.com/bnb-chain/reth/pull/70) fix: remove sidecars when calling block_with_senders
* [\#64](https://github.com/bnb-chain/reth/pull/64) fix: create empty sidecars file if no sidecars before
* [\#65](https://github.com/bnb-chain/reth/pull/65) fix: hertz storage patch issue on bsc mainnet
* [\#59](https://github.com/bnb-chain/reth/pull/59) fix: add check of plato hardfork in verify_vote_attestation
* [\#60](https://github.com/bnb-chain/reth/pull/60) fix: zero block reward issue
* [\#57](https://github.com/bnb-chain/reth/pull/57) fix: fix the hardforks' order
* [\#52](https://github.com/bnb-chain/reth/pull/52) fix: spec mapping issue and snapshot overwrite issue
* [\#53](https://github.com/bnb-chain/reth/pull/53) fix: system account status issue

## v1.0.0-rc.1

### FEATURES

* [\#38](https://github.com/bnb-chain/reth/pull/38) chore: merge v1.0.0 into develop
* [\#39](https://github.com/bnb-chain/reth/pull/39) feat: add bootnodes for opbnb
* [\#41](https://github.com/bnb-chain/reth/pull/41) feat: add bootnodes for opbnb
* [\#42](https://github.com/bnb-chain/reth/pull/42) chore: add HaberFix upgrade and other optimization
* [\#43](https://github.com/bnb-chain/reth/pull/43) feat: add docker file for optimism and opbnb
* [\#44](https://github.com/bnb-chain/reth/pull/44) chore: add docker image workflow for bsc
* [\#45](https://github.com/bnb-chain/reth/pull/45) feat: add support of block sidecars for bsc

### BUGFIX
* [\#34](https://github.com/bnb-chain/reth/pull/34) fix: update executor type in commands for bsc
* [\#36](https://github.com/bnb-chain/reth/pull/36) fix: base fee configuration of bsc
* [\#40](https://github.com/bnb-chain/reth/pull/40) fix: unwrap failed on fcu_resp
* [\#46](https://github.com/bnb-chain/reth/pull/46) fix: check header timestamp in parlia task

## v0.1.0-beta.3

### FEATURE
* [\#33](https://github.com/bnb-chain/reth/pull/33) feat: enable Shanghai, Canyon, Cancun, Ecotone, Haber on opBNB mainnet

## v0.1.0-beta.2
### BUGFIX
* [\#25](https://github.com/bnb-chain/reth/pull/25) chore: add pr template
* [\#27](https://github.com/bnb-chain/reth/pull/27) feat: introduce Haber fork into opBNB testnet
* [\#26](https://github.com/bnb-chain/reth/pull/26) fix: opbnb p2p forkid mismatch issue
* [\#24](https://github.com/bnb-chain/reth/pull/24) fix: opbnb synchronization failure issue
* [\#19](https://github.com/bnb-chain/reth/pull/19) chore: fix ci issues
* [\#17](https://github.com/bnb-chain/reth/pull/17) fix p2p incompatible forks for opbnb testnet and mainnet

## v0.1.0-beta.1

### FEATURE
* [\#10](https://github.com/bnb-chain/reth/pull/10) feat: support opbnb network
