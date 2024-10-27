use super::{macros::define_plan_node, Expr, DfReprPlanNode, ArcDfPlanNode, DfNodeType, DfReprPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalLimit(pub DfReprPlanNode);

define_plan_node!(
    LogicalLimit : DfReprPlanNode,
    Limit, [
        { 0, child: DfReprPlanNode }
    ], [
        { 1, skip: Expr },
        { 2, fetch: Expr }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalLimit(pub DfReprPlanNode);

define_plan_node!(
    PhysicalLimit : DfReprPlanNode,
    PhysicalLimit, [
        { 0, child: DfReprPlanNode }
    ], [
        { 1, skip: Expr },
        { 2, fetch: Expr }
    ]
);
