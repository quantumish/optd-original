use crate::{
    cascades::{CascadesOptimizer, RelNodeContext},
    rel_node::{RelNode, RelNodeTyp, Value},
    physical_prop::PhysicalPropsBuilder,
};

#[derive(Default, Clone, Debug, PartialOrd, PartialEq)]
pub struct Cost(pub Vec<f64>);

pub trait CostModel<T: RelNodeTyp, P:PhysicalPropsBuilder<T>>: 'static + Send + Sync {
    fn compute_cost(
        &self,
        node: &T,
        data: &Option<Value>,
        children: &[Cost],
        context: Option<RelNodeContext>,
        // one reason we need the optimizer is to traverse children nodes to build up an expression tree
        optimizer: Option<&CascadesOptimizer<T,P>>,
    ) -> Cost;

    fn compute_plan_node_cost(&self, node: &RelNode<T>) -> Cost;

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
