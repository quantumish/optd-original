use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{DfReprPlanNode, ArcDfPlanNode, DfNodeType, DfReprPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalProjection(pub DfReprPlanNode);

define_plan_node!(
    LogicalProjection : DfReprPlanNode,
    Projection, [
        { 0, child: DfReprPlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalProjection(pub DfReprPlanNode);

define_plan_node!(
    PhysicalProjection : DfReprPlanNode,
    PhysicalProjection, [
        { 0, child: DfReprPlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);
