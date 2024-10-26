use optd_core::node::{PlanNode, PlanNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

use super::ExprList;

#[derive(Clone, Debug)]
pub struct InListExpr(pub Expr);

impl InListExpr {
    pub fn new(expr: Expr, list: ExprList, negated: bool) -> Self {
        InListExpr(Expr(
            PlanNode {
                typ: OptRelNodeTyp::InList,
                children: vec![expr.into_rel_node(), list.into_rel_node()],
                data: Some(Value::Bool(negated)),
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn list(&self) -> ExprList {
        ExprList::from_rel_node(self.0.child(1)).unwrap()
    }

    /// `true` for `NOT IN`.
    pub fn negated(&self) -> bool {
        self.0 .0.data.as_ref().unwrap().as_bool()
    }
}

impl OptRelNode for InListExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::InList) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "InList",
            vec![
                ("expr", self.child().explain(meta_map)),
                ("list", self.list().explain(meta_map)),
                ("negated", self.negated().to_string().into()),
            ],
            vec![],
        )
    }
}
