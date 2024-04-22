use anyhow::Result;

use crate::{physical_prop::PhysicalPropsBuilder, rel_node::RelNodeTyp};

use super::CascadesOptimizer;

mod apply_rule;
mod explore_group;
mod optimize_expression;
mod optimize_group;
mod optimize_inputs;

pub use apply_rule::ApplyRuleTask;
pub use explore_group::ExploreGroupTask;
pub use optimize_expression::OptimizeExpressionTask;
pub use optimize_group::OptimizeGroupTask;
pub use optimize_inputs::OptimizeInputsTask;

pub trait Task<T: RelNodeTyp, P: PhysicalPropsBuilder<T>>: 'static + Send + Sync {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, P>) -> Result<Vec<Box<dyn Task<T, P>>>>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn describe(&self) -> String;
}
