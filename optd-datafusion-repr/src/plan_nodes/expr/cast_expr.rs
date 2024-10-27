use arrow_schema::DataType;
use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{DfNodeType, Expr, DfReprPlanNode, ArcDfPlanNode};

use super::DataTypeExpr;

#[derive(Clone, Debug)]
pub struct CastExpr(pub Expr);

impl CastExpr {
    pub fn new(expr: Expr, cast_to: DataType) -> Self {
        CastExpr(Expr(
            PlanNode {
                typ: DfNodeType::Cast,
                children: vec![
                    expr.into_rel_node(),
                    DataTypeExpr::new(cast_to).into_rel_node(),
                ],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn cast_to(&self) -> DataType {
        DataTypeExpr::from_rel_node(self.0.child(1))
            .unwrap()
            .data_type()
    }
}

impl DfReprPlanNode for CastExpr {
    fn into_rel_node(self) -> ArcDfPlanNode {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: ArcDfPlanNode) -> Option<Self> {
        if !matches!(rel_node.typ, DfNodeType::Cast) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "Cast",
            vec![
                ("cast_to", format!("{}", self.cast_to()).into()),
                ("expr", self.child().explain(meta_map)),
            ],
            vec![],
        )
    }
}
