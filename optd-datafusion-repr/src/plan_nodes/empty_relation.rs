use core::fmt;
use std::fmt::Display;

use super::macros::define_plan_node;
use super::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum EmptyRelationType {
    Empty = 1,
    OneRow = 2,
}

impl Display for EmptyRelationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalEmptyRelation(pub PlanNode);

define_plan_node!(
    LogicalEmptyRelation : PlanNode,
    EmptyRelation, [
        { 0, child: PlanNode }
    ], [], { rel_type: EmptyRelationType }
);

#[derive(Clone, Debug)]
pub struct PhysicalEmptyRelation(pub PlanNode);

define_plan_node!(
    PhysicalEmptyRelation : PlanNode,
    PhysicalEmptyRelation, [
        { 0, child: PlanNode }
    ], [], { rel_type: EmptyRelationType }
);
