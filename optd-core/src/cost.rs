use crate::{
    cascades::{CascadesOptimizer, RelNodeContext},
    nodes::{ArcPredNode, NodeType, PlanNode, Value},
};

#[derive(Default, Clone, Debug, PartialOrd, PartialEq)]
pub struct Cost(pub Vec<f64>);

pub trait CostModel<T: NodeType>: 'static + Send + Sync {
    fn compute_cost(
        &self,
        node: &T,
        predicates: &[ArcPredNode<T>],
        children_costs: &[Cost],
        context: Option<RelNodeContext>,
        // one reason we need the optimizer is to traverse children nodes to build up an expression tree
        optimizer: Option<&CascadesOptimizer<T>>,
    ) -> Cost;

    // TODO: Rename—confusing w/ predicates
    fn compute_plan_node_cost(&self, node: &PlanNode<T>) -> Cost;

    fn explain(&self, cost: &Cost) -> String;

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost);

    fn sum(&self, self_cost: &Cost, inputs: &[Cost]) -> Cost {
        let mut total_cost = self_cost.clone();
        for input in inputs {
            self.accumulate(&mut total_cost, input);
        }
        total_cost
    }

    fn zero(&self) -> Cost;
}
