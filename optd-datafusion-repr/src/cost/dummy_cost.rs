use crate::plan_nodes::OptRelNodeTyp;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, Value},
};

/// Dummy cost model that returns a 0 cost in all cases. Intended for testing.
pub struct DummyCostModel;

impl CostModel<OptRelNodeTyp> for DummyCostModel {
    fn compute_cost(
        &self,
        node: &OptRelNodeTyp,
        data: &Option<Value>,
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        Cost(vec![0.0])
    }

    fn compute_plan_node_cost(&self, node: &RelNode<OptRelNodeTyp>) -> Cost {
        Cost(vec![0.0])
    }

    fn explain(&self, node: &Cost) -> String {
        "Dummy cost".to_string()
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {}

    fn zero(&self) -> Cost {
        Cost(vec![0.0])
    }
}
