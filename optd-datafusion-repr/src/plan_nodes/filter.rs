use super::macros::define_plan_node;

use super::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, DfPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalFilter(pub DfPlanNode);

define_plan_node!(
    LogicalFilter : DfPlanNode,
    Filter, [
        { 0, child: DfPlanNode }
    ], [
        { 1, cond: Expr }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalFilter(pub DfPlanNode);

define_plan_node!(
    PhysicalFilter : DfPlanNode,
    PhysicalFilter, [
        { 0, child: DfPlanNode }
    ], [
        { 1, cond: Expr }
    ]
);
