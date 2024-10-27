use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{DfNodeType, Expr, DfReprPlanNode, ArcDfPlanNode};

#[derive(Clone, Debug)]
pub struct BetweenExpr(pub Expr);

impl BetweenExpr {
    pub fn new(expr: Expr, lower: Expr, upper: Expr) -> Self {
        BetweenExpr(Expr(
            PlanNode {
                typ: DfNodeType::Between,
                children: vec![
                    expr.into_rel_node(),
                    lower.into_rel_node(),
                    upper.into_rel_node(),
                ],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn lower(&self) -> Expr {
        Expr(self.0.child(1))
    }

    pub fn upper(&self) -> Expr {
        Expr(self.0.child(2))
    }
}

impl DfReprPlanNode for BetweenExpr {
    fn into_rel_node(self) -> ArcDfPlanNode {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: ArcDfPlanNode) -> Option<Self> {
        if !matches!(rel_node.typ, DfNodeType::Between) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "Between",
            vec![
                ("expr", self.child().explain(meta_map)),
                ("lower", self.lower().explain(meta_map)),
                ("upper", self.upper().explain(meta_map)),
            ],
            vec![],
        )
    }
}
