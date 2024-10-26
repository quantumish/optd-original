use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{DfPlanNode, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct LogicalAgg(pub DfPlanNode);

define_plan_node!(
    LogicalAgg : DfPlanNode,
    Agg, [
        { 0, child: DfPlanNode }
    ], [
        { 1, exprs: ExprList },
        { 2, groups: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalAgg(pub DfPlanNode);

define_plan_node!(
    PhysicalAgg : DfPlanNode,
    PhysicalAgg, [
        { 0, child: DfPlanNode }
    ], [
        { 1, aggrs: ExprList },
        { 2, groups: ExprList }
    ]
);
