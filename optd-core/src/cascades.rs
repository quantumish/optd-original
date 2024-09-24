//! The core cascades optimizer implementation.

mod memo;
mod optimizer;
mod tasks;

pub use optimizer::{CascadesOptimizer, GroupId, RelNodeContext};
