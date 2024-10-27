use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{DfNodeType, Expr, DfReprPlanNode, ArcDfPlanNode};

use super::ExprList;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FuncType {
    Scalar(datafusion_expr::BuiltinScalarFunction),
    Agg(datafusion_expr::AggregateFunction),
    Case,
}

impl std::fmt::Display for FuncType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FuncType {
    pub fn new_scalar(func_id: datafusion_expr::BuiltinScalarFunction) -> Self {
        FuncType::Scalar(func_id)
    }

    pub fn new_agg(func_id: datafusion_expr::AggregateFunction) -> Self {
        FuncType::Agg(func_id)
    }
}

#[derive(Clone, Debug)]
pub struct FuncExpr(Expr);

impl FuncExpr {
    pub fn new(func_id: FuncType, argv: ExprList) -> Self {
        FuncExpr(Expr(
            PlanNode {
                typ: DfNodeType::Func(func_id),
                children: vec![argv.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    /// Gets the i-th argument of the function.
    pub fn arg_at(&self, i: usize) -> Expr {
        self.children().child(i)
    }

    /// Get all children.
    pub fn children(&self) -> ExprList {
        ExprList::from_rel_node(self.0.child(0)).unwrap()
    }

    /// Gets the function id.
    pub fn func(&self) -> FuncType {
        if let DfNodeType::Func(func_id) = &self.clone().into_rel_node().typ {
            func_id.clone()
        } else {
            panic!("not a function")
        }
    }
}

impl DfReprPlanNode for FuncExpr {
    fn into_rel_node(self) -> ArcDfPlanNode {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: ArcDfPlanNode) -> Option<Self> {
        if !matches!(rel_node.typ, DfNodeType::Func(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.func().to_string(),
            vec![],
            vec![self.children().explain(meta_map)],
        )
    }
}
