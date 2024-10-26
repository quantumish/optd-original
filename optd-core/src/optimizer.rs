use anyhow::Result;

use crate::{
    node::{NodeType, RelNodeRef},
    property::PropertyBuilder,
};

pub trait Optimizer<T: NodeType> {
    fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>>;
    fn get_property<P: PropertyBuilder<T>>(&self, root_rel: RelNodeRef<T>, idx: usize) -> P::Prop;
}
