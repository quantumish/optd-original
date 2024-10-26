use super::{macros::define_plan_node, Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, DfPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalLimit(pub DfPlanNode);

define_plan_node!(
    LogicalLimit : DfPlanNode,
    Limit, [
        { 0, child: DfPlanNode }
    ], [
        { 1, skip: Expr },
        { 2, fetch: Expr }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalLimit(pub DfPlanNode);

define_plan_node!(
    PhysicalLimit : DfPlanNode,
    PhysicalLimit, [
        { 0, child: DfPlanNode }
    ], [
        { 1, skip: Expr },
        { 2, fetch: Expr }
    ]
);
