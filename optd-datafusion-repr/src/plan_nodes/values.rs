use pretty_xmlish::Pretty;

use optd_core::rel_node::{RelNode, RelNodeMetaMap, Value};

use super::{replace_typ, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalValues(pub PlanNode);

impl OptRelNode for LogicalValues {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Values {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::childless_record(
            "LogicalValues",
            vec![("values", self.values().to_string().into())],
        )
    }
}

impl LogicalValues {
    pub fn new(values: Vec<Vec<Value>>) -> LogicalValues {
        // TODO: verify if the values field is consistent
        LogicalValues(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::Values,
                children: vec![],
                data: Some(Value::List(
                    values.into_iter().map(|x| Value::List(x.into())).collect(),
                )),
            }
            .into(),
        ))
    }

    /// Returns a table of values
    pub fn values(&self) -> &Value {
        self.0 .0.data.as_ref().unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalValues(pub PlanNode);

impl OptRelNode for PhysicalValues {
    fn into_rel_node(self) -> OptRelNodeRef {
        replace_typ(self.0.into_rel_node(), OptRelNodeTyp::PhysicalValues)
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::PhysicalValues {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::childless_record(
            "PhysicalValues",
            vec![("values", self.values().to_string().into())],
        )
    }
}

impl PhysicalValues {
    pub fn new(node: PlanNode) -> PhysicalValues {
        Self(node)
    }

    /// Returns a table of values
    pub fn values(&self) -> &Value {
        self.0 .0.data.as_ref().unwrap()
    }
}
