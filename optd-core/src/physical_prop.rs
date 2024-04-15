use std::hash::Hash;
use std::cmp::{Eq, PartialEq};
use std::fmt::Debug;
use crate::rel_node::{RelNodeTyp, Value, RelNodeRef};

pub trait PhysicalPropsBuilder<T: RelNodeTyp>: 'static + Send + Sync{

    type PhysicalProps: 'static + Send + Sync + Sized + Clone + Debug + Eq + PartialEq + Hash;

    fn names(&self, props: Self::PhysicalProps) -> Vec<&'static str>;

    fn is_any(&self, props: Self::PhysicalProps) -> bool;

    fn any(&self) -> Self::PhysicalProps;

    fn can_provide(
        &self,
        typ: T,
        data: Option<Value>,
        required: &Self::PhysicalProps
    ) -> bool;

    fn build_children_properties(
        &self,
        typ: T,
        data: Option<Value>,
        children_len: usize,
        required: &Self::PhysicalProps
    ) -> Vec<Self::PhysicalProps>;

    fn enforce(
        &self,
        expr: RelNodeRef<T>,
        required: &Self::PhysicalProps
    ) -> RelNodeRef<T>;
}