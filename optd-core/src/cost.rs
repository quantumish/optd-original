use crate::{
    cascades::{CascadesOptimizer, Memo, RelNodeContext},
    nodes::{ArcPredNode, NodeType, PlanNode, Value},
};

/// The statistics of a group.
#[derive(Clone, Debug)]
pub struct Statistics(pub value_bag::OwnedValueBag);

/// The cost of an operation. The cost is represented as a vector of double values.
/// For example, it can be represented as `[compute_cost, io_cost]`.
/// A lower value means a better cost.
#[derive(Default, Clone, Debug, PartialOrd, PartialEq)]
pub struct Cost(pub Vec<f64>);

pub trait CostModel<T: NodeType, M: Memo<T>>: 'static + Send + Sync {
    /// Compute the cost of a single operation
    fn compute_operation_cost(
        &self,
        node: &T,
        children_stat: &[Option<&Statistics>],
        predicates: &[ArcPredNode<T>],
        children_cost: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<T, M>>,
    ) -> Cost;

    /// Derive the statistics of a single operation
    fn derive_statistics(
        &self,
        node: &T,
        children_stat: &[&Statistics],
        predicates: &[ArcPredNode<T>],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<T, M>>,
    ) -> Statistics;

    fn explain_cost(&self, cost: &Cost) -> String;

    fn explain_statistics(&self, cost: &Statistics) -> String;

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost);

    fn sum(&self, operation_cost: &Cost, inputs_cost: &[Cost]) -> Cost {
        let mut total_cost = operation_cost.clone();
        for input in inputs_cost {
            self.accumulate(&mut total_cost, input);
        }
        total_cost
    }

    /// The zero cost.
    fn zero(&self) -> Cost;

    /// The weighted cost of a compound cost.
    fn weighted_cost(&self, cost: &Cost) -> f64;
}
