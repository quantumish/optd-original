use optd_core::nodes::{PlanNode, PlanNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use super::macros::define_plan_node;
use super::{ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfReprPlanNode, JoinType};

#[derive(Clone, Debug)]
pub struct RawDependentJoin(pub ArcDfPlanNode);

define_plan_node!(
    RawDependentJoin : DfReprPlanNode,
    RawDepJoin, [
        { 0, left: ArcDfPlanNode },
        { 1, right: ArcDfPlanNode }
    ], [
        { 0, cond: ArcDfPredNode },
        { 1, extern_cols: ArcDfPredNode }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct DependentJoin(pub ArcDfPlanNode);

define_plan_node!(
    DependentJoin : DfReprPlanNode,
    DepJoin, [
        { 0, left: ArcDfPlanNode },
        { 1, right: ArcDfPlanNode }
    ], [
        { 0, cond: ArcDfPredNode },
        { 1, extern_cols: ArcDfPredNode }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct ExternColumnRefExpr(pub Expr);

impl ExternColumnRefExpr {
    /// Creates a new `DepExternColumnRef` expression.
    pub fn new(column_idx: usize) -> ExternColumnRefExpr {
        // this conversion is always safe since usize is at most u64
        let u64_column_idx = column_idx as u64;
        ExternColumnRefExpr(Expr(
            PlanNode {
                typ: DfNodeType::ExternColumnRef,
                children: vec![],
                data: Some(Value::UInt64(u64_column_idx)),
            }
            .into(),
        ))
    }

    fn get_data_usize(&self) -> usize {
        self.0 .0.data.as_ref().unwrap().as_u64() as usize
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        self.get_data_usize()
    }
}

impl DfReprPlanNode for ExternColumnRefExpr {
    fn into_rel_node(self) -> ArcDfPlanNode {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: ArcDfPlanNode) -> Option<Self> {
        if rel_node.typ != DfNodeType::ExternColumnRef {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&format!("Extern(#{})", self.index()))
    }
}
