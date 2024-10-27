use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{ArcDfPlanNode, DfNodeType, DfPlanNode, DfReprPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalAgg(pub ArcDfPlanNode);

define_plan_node!(
    LogicalAgg : DfPlanNode,
    Agg, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 1, exprs: ExprList },
        { 2, groups: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalAgg(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalAgg : DfPlanNode,
    PhysicalAgg, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 1, aggrs: ExprList },
        { 2, groups: ExprList }
    ]
);
