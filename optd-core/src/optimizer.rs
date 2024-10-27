use anyhow::Result;

use crate::{
    node::{NodeType, ArcPlanNode},
    property::PropertyBuilder,
};

pub trait Optimizer<T: NodeType> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>>;
    fn get_property<P: PropertyBuilder<T>>(&self, root_rel: ArcPlanNode<T>, idx: usize) -> P::Prop;
}
