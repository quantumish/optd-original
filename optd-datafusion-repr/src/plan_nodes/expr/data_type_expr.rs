use arrow_schema::DataType;
use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{DfNodeType, Expr, DfReprPlanNode, ArcDfPlanNode};

#[derive(Clone, Debug)]
pub struct DataTypeExpr(pub Expr);

impl DataTypeExpr {
    pub fn new(typ: DataType) -> Self {
        DataTypeExpr(Expr(
            PlanNode {
                typ: DfNodeType::DataType(typ),
                children: vec![],
                data: None,
            }
            .into(),
        ))
    }

    pub fn data_type(&self) -> DataType {
        if let DfNodeType::DataType(data_type) = self.0.typ() {
            data_type
        } else {
            panic!("not a data type")
        }
    }
}

impl DfReprPlanNode for DataTypeExpr {
    fn into_rel_node(self) -> ArcDfPlanNode {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: ArcDfPlanNode) -> Option<Self> {
        if !matches!(rel_node.typ, DfNodeType::DataType(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&self.data_type().to_string())
    }
}
