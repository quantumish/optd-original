use crate::node::{NodeType, Value};
use std::{any::Any, fmt::Debug};

pub trait PropertyBuilderAny<T: NodeType>: 'static + Send + Sync {
    fn derive_any(
        &self,
        typ: T,
        data: Option<Value>,
        children: &[&dyn Any],
    ) -> Box<dyn Any + Send + Sync + 'static>;
    fn display(&self, prop: &dyn Any) -> String;
    fn property_name(&self) -> &'static str;
}

pub trait PropertyBuilder<T: NodeType>: 'static + Send + Sync + Sized {
    type Prop: 'static + Send + Sync + Sized + Clone + Debug;
    fn derive(&self, typ: T, data: Option<Value>, children: &[&Self::Prop]) -> Self::Prop;
    fn property_name(&self) -> &'static str;
}

impl<T: NodeType, P: PropertyBuilder<T>> PropertyBuilderAny<T> for P {
    fn derive_any(
        &self,
        typ: T,
        data: Option<Value>,
        children: &[&dyn Any],
    ) -> Box<dyn Any + Send + Sync + 'static> {
        let children: Vec<&P::Prop> = children
            .iter()
            .map(|child| {
                child
                    .downcast_ref::<P::Prop>()
                    .expect("Failed to downcast child")
            })
            .collect();
        Box::new(self.derive(typ, data, &children))
    }

    fn display(&self, prop: &dyn Any) -> String {
        let prop = prop
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property");
        format!("{:?}", prop)
    }

    fn property_name(&self) -> &'static str {
        PropertyBuilder::property_name(self)
    }
}
