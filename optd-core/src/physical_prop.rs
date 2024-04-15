use std::sync::Arc;
use crate::rel_node::{RelNodeTyp, Value, RelNodeRef};

pub trait PhysicalProps<T: RelNodeTyp>: 'static + Send + Sync{
    fn names(&self) -> Vec<&'static str>;
    fn is_any(&self) -> bool;
    fn any(&self) -> Arc<dyn PhysicalProps<T>>;
    fn can_provide(
        &self,
        typ: T,
        data: Option<Value>
    ) -> bool;

    fn build_children_properties(
        &self,
        typ: T,
        data: Option<Value>,
        children_len: usize
    ) -> Vec<Arc<dyn PhysicalProps<T>>>;

    fn enforce(
        &self,
        expr: RelNodeRef<T>,
    ) -> RelNodeRef<T>;
}