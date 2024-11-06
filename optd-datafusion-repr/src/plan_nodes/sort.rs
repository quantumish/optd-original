use optd_core::nodes::PlanNodeOrGroup;

use super::macros::define_plan_node;
use super::predicates::ListPred;

use super::{
    ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfPlanNodeOrGroup, DfReprPlanNode,
};

#[derive(Clone, Debug)]
pub struct LogicalSort(pub ArcDfPlanNode);

// each expression in ExprList is represented as a SortOrderExpr
// 1. nulls_first is not included from DF
// 2. node type defines sort order per expression
// 3. actual expr is stored as a child of this node
define_plan_node!(
    LogicalSort : DfNodeType,
    Sort, [
        { 0, child: DfPlanNodeOrGroup }
    ], [
        { 0, exprs: ArcDfPredNode }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalSort(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalSort : DfNodeType,
    PhysicalSort, [
        { 0, child: DfPlanNodeOrGroup }
    ], [
        { 0, exprs: ArcDfPredNode }
    ]
);
