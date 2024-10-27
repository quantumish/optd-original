use super::macros::define_plan_node;

use super::{ArcDfPlanNode, DfNodeType, DfReprPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalFilter(pub ArcDfPlanNode);

define_plan_node!(
    LogicalFilter : DfPlanNode,
    Filter, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 1, cond: Expr }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalFilter(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalFilter : DfPlanNode,
    PhysicalFilter, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 1, cond: Expr }
    ]
);
