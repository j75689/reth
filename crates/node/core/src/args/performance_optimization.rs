//! Performance optimization arguments

use clap::Args;

/// Parameters for performance optimization
#[derive(Debug, Clone, Args, PartialEq, Eq, Default)]
#[command(next_help_heading = "Performance Optimization")]
pub struct PerformanceOptimizationArgs {
    /// Skip state root validation during block import.
    /// This flag is useful for performance optimization when importing blocks from trusted sources.
    #[arg(long = "skip-state-root-validation", default_value_t = false)]
    pub skip_state_root_validation: bool,
}
