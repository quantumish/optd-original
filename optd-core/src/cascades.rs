//! The core cascades optimizer implementation.

mod memo;
mod optimizer;
mod tasks;

pub use memo::{BindingType, Memo, NaiveMemo};
pub use optimizer::{CascadesOptimizer, ExprId, GroupId, RelNodeContext};
