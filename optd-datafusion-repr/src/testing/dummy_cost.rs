use crate::plan_nodes::{ArcDfPredNode, DfNodeType};
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel},
    nodes::{PlanNode, Value},
};

/// Dummy cost model that returns a 0 cost in all cases.
/// Intended for testing with the cascades optimizer.
pub struct DummyCostModel;

impl CostModel<DfNodeType> for DummyCostModel {
    fn compute_cost(
        &self,
        _node: &DfNodeType,
        _predicates: &[ArcDfPredNode],
        _children_costs: &[Cost],
        _context: Option<RelNodeContext>,
        _optimizer: Option<&CascadesOptimizer<DfNodeType>>,
    ) -> Cost {
        Cost(vec![0.0])
    }

    fn compute_plan_node_cost(&self, _node: &PlanNode<DfNodeType>) -> Cost {
        Cost(vec![0.0])
    }

    fn explain(&self, _node: &Cost) -> String {
        "Dummy cost".to_string()
    }

    fn accumulate(&self, _total_cost: &mut Cost, _cost: &Cost) {}

    fn zero(&self) -> Cost {
        Cost(vec![0.0])
    }
}
