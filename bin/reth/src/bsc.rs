#![allow(missing_docs)]

// We use jemalloc for performance reasons.
#[cfg(all(feature = "jemalloc", unix))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(feature = "bsc"))]
compile_error!("Cannot build the `bsc-reth` binary with the `bsc` feature flag disabled.");

/// clap [Args] for Engine related arguments.
use clap::Args;

/// Parameters for configuring the engine
#[derive(Debug, Clone, Args, PartialEq, Eq, Default)]
#[command(next_help_heading = "Engine")]
pub struct EngineArgs {
    /// Enable the engine2 experimental features on reth binary
    #[arg(long = "engine.experimental", default_value = "false")]
    pub experimental: bool,
}

#[cfg(feature = "bsc")]
fn main() {
    use clap::Parser;
    use reth::cli::Cli;
    use reth_node_bsc::{node::BSCAddOns, BscNode};
    use reth_node_builder::EngineNodeLauncher;
    use reth_provider::providers::BlockchainProvider2;

    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    if let Err(err) = Cli::<EngineArgs>::parse().run(|builder, engine_args| async move {
        let enable_engine2 = engine_args.experimental;
        match enable_engine2 {
            true => {
                let handle = builder
                    .with_types_and_provider::<BscNode, BlockchainProvider2<_>>()
                    .with_components(BscNode::components())
                    .with_add_ons::<BSCAddOns>()
                    .launch_with_fn(|builder| {
                        let launcher = EngineNodeLauncher::new(
                            builder.task_executor().clone(),
                            builder.config().datadir(),
                        );
                        builder.launch_with(launcher)
                    })
                    .await?;
                handle.node_exit_future.await
            }
            false => {
                let handle = builder.launch_node(BscNode::default()).await?;
                handle.node_exit_future.await
            }
        }
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
