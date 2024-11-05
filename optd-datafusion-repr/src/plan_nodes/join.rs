use core::fmt;
use std::fmt::Display;

use super::macros::define_plan_node;
use super::{ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfReprPlanNode};

use crate::plan_nodes::{dispatch_plan_explain, dispatch_pred_explain, get_meta};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner = 1,
    FullOuter,
    LeftOuter,
    RightOuter,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalJoin(pub ArcDfPlanNode);

define_plan_node!(
    LogicalJoin : DfPlanNode,
    Join, [
        { 0, left: ArcDfPlanNode },
        { 1, right: ArcDfPlanNode }
    ], [
        { 0, cond: ArcDfPredNode }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct PhysicalNestedLoopJoin(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalNestedLoopJoin : DfPlanNode,
    PhysicalNestedLoopJoin, [
        { 0, left: ArcDfPlanNode },
        { 1, right: ArcDfPlanNode }
    ], [
        { 0, cond: ArcDfPredNode }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct PhysicalHashJoin(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalHashJoin : DfPlanNode,
    PhysicalHashJoin, [
        { 0, left: ArcDfPlanNode },
        { 1, right: ArcDfPlanNode }
    ], [
        { 0, left_keys: ArcDfPredNode },
        { 1, right_keys: ArcDfPredNode }
    ], { join_type: JoinType }
);

impl LogicalJoin {
    /// Takes in left/right schema sizes, and maps a column index to be as if it
    /// were pushed down to the left or right side of a join accordingly.
    pub fn map_through_join(
        col_idx: usize,
        left_schema_size: usize,
        right_schema_size: usize,
    ) -> usize {
        assert!(col_idx < left_schema_size + right_schema_size);
        if col_idx < left_schema_size {
            col_idx
        } else {
            col_idx - left_schema_size
        }
    }
}
