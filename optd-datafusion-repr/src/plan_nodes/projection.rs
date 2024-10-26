use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, DfPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalProjection(pub DfPlanNode);

define_plan_node!(
    LogicalProjection : DfPlanNode,
    Projection, [
        { 0, child: DfPlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalProjection(pub DfPlanNode);

define_plan_node!(
    PhysicalProjection : DfPlanNode,
    PhysicalProjection, [
        { 0, child: DfPlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);
