use arrow_schema::DataType;
use optd_core::node::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct DataTypeExpr(pub Expr);

impl DataTypeExpr {
    pub fn new(typ: DataType) -> Self {
        DataTypeExpr(Expr(
            PlanNode {
                typ: OptRelNodeTyp::DataType(typ),
                children: vec![],
                data: None,
            }
            .into(),
        ))
    }

    pub fn data_type(&self) -> DataType {
        if let OptRelNodeTyp::DataType(data_type) = self.0.typ() {
            data_type
        } else {
            panic!("not a data type")
        }
    }
}

impl OptRelNode for DataTypeExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::DataType(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&self.data_type().to_string())
    }
}
