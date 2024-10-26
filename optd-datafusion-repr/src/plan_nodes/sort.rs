use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{DfPlanNode, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct LogicalSort(pub DfPlanNode);

// each expression in ExprList is represented as a SortOrderExpr
// 1. nulls_first is not included from DF
// 2. node type defines sort order per expression
// 3. actual expr is stored as a child of this node
define_plan_node!(
    LogicalSort : DfPlanNode,
    Sort, [
        { 0, child: DfPlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalSort(pub DfPlanNode);

define_plan_node!(
    PhysicalSort : DfPlanNode,
    PhysicalSort, [
        { 0, child: DfPlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);
