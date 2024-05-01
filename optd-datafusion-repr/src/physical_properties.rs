use optd_core::{
    rel_node::{RelNodeRef, Value},
    physical_prop::PhysicalPropsBuilder
};
use crate::OptRelNodeTyp;

// Use this type to choose the physical properties that are registered for the optimizer
pub type PhysicalPropsBuilderImpl = EmptyPhysicalPropsBuilder;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum EmptyPhysicalPropState{
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmptyPhysicalProps(EmptyPhysicalPropState);

pub struct EmptyPhysicalPropsBuilder{
    state: EmptyPhysicalPropState,
}

impl PhysicalPropsBuilder<OptRelNodeTyp> for EmptyPhysicalPropsBuilder{
    type PhysicalProps = EmptyPhysicalProps;

    fn new () -> Self{
        EmptyPhysicalPropsBuilder{
            state: EmptyPhysicalPropState::Empty
        }
    }

    fn names(&self, props: &Self::PhysicalProps) -> Vec<&'static str>{
        vec!["EmptyPhysicalProps"]
    }

    fn is_any(&self, props: &Self::PhysicalProps) -> bool{
        match props.0{
            EmptyPhysicalPropState::Empty => true,
        }
    }

    fn any(&self) -> Self::PhysicalProps{
        EmptyPhysicalProps(EmptyPhysicalPropState::Empty)
    }

    fn can_provide(
        &self,
        typ: &OptRelNodeTyp,
        data: &Option<Value>,
        required: &Self::PhysicalProps
    ) -> bool{
        self.is_any(required)
    }

    fn build_children_properties(
        &self,
        typ: &OptRelNodeTyp,
        data: &Option<Value>,
        children_len: usize,
        required: &Self::PhysicalProps
    ) -> Vec<Self::PhysicalProps>{
        vec![self.any(); children_len]
    }

    fn enforce(
        &self,
        expr: RelNodeRef<OptRelNodeTyp>,
        required: &Self::PhysicalProps
    ) -> RelNodeRef<OptRelNodeTyp>{
        expr
    }

    fn separate_physical_props(
        &self,
        typ: &OptRelNodeTyp,
        data: &Option<Value>,
        required: &Self::PhysicalProps,
        children_len: usize,
    ) -> Vec<(Self::PhysicalProps, Self::PhysicalProps, Vec<Self::PhysicalProps>)>{
        let pass_to_children = self.any();
        let enforcer = self.any();
        let children_props = vec![self.any(); children_len];
        vec![(pass_to_children, enforcer, children_props)]
    }
}