use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{DfReprPlanNode, DfReprPlanNode, ArcDfPlanNode, DfNodeType};

#[derive(Clone, Debug)]
pub struct LogicalSort(pub DfReprPlanNode);

// each expression in ExprList is represented as a SortOrderExpr
// 1. nulls_first is not included from DF
// 2. node type defines sort order per expression
// 3. actual expr is stored as a child of this node
define_plan_node!(
    LogicalSort : DfReprPlanNode,
    Sort, [
        { 0, child: DfReprPlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalSort(pub DfReprPlanNode);

define_plan_node!(
    PhysicalSort : DfReprPlanNode,
    PhysicalSort, [
        { 0, child: DfReprPlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);
