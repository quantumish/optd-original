use crate::plan_nodes::DfNodeType;
use optd_core::{
    cascades::{CascadesOptimizer, NaiveMemo, RelNodeContext},
    cost::{Cost, CostModel, Statistics},
    nodes::ArcPredNode,
};
use value_bag::ValueBag;

/// Dummy cost model that returns a 0 cost in all cases.
/// Intended for testing with the cascades optimizer.
pub struct DummyCostModel;

impl CostModel<DfNodeType, NaiveMemo<DfNodeType>> for DummyCostModel {
    /// Compute the cost of a single operation
    fn compute_operation_cost(
        &self,
        _: &DfNodeType,
        _: &[Option<&Statistics>],
        _: &[ArcPredNode<DfNodeType>],
        _: &[Cost],
        _: Option<RelNodeContext>,
        _: Option<&CascadesOptimizer<DfNodeType, NaiveMemo<DfNodeType>>>,
    ) -> Cost {
        Cost(vec![1.0])
    }

    /// Derive the statistics of a single operation
    fn derive_statistics(
        &self,
        _: &DfNodeType,
        _: &[&Statistics],
        _: &[ArcPredNode<DfNodeType>],
        _: Option<RelNodeContext>,
        _: Option<&CascadesOptimizer<DfNodeType, NaiveMemo<DfNodeType>>>,
    ) -> Statistics {
        Statistics(ValueBag::empty().to_owned())
    }

    fn explain_cost(&self, _: &Cost) -> String {
        "dummy_cost".to_string()
    }

    fn explain_statistics(&self, _: &Statistics) -> String {
        "dummy_statistics".to_string()
    }

    fn weighted_cost(&self, cost: &Cost) -> f64 {
        cost.0[0]
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {
        total_cost.0[0] += cost.0[0];
    }

    fn zero(&self) -> Cost {
        Cost(vec![0.0])
    }
}
