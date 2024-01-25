mod ir;

use std::collections::HashMap;

use crate::{
    optimizer::Optimizer,
    rel_node::{RelNode, RelNodeTyp},
};

pub use ir::RuleMatcher;

/// A rule can be two kind:
/// 1. transformation rule fired to expand expression, generating new logical multi-expressions
/// 2. implementation rule fired to generating corresponding physical multi-expressions.
pub trait Rule<T: RelNodeTyp, O: Optimizer<T>>: 'static + Send + Sync {
    fn matcher(&self) -> &RuleMatcher<T>;
    fn apply(&self, optimizer: &O, input: HashMap<usize, RelNode<T>>) -> Vec<RelNode<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        false
    }
}
