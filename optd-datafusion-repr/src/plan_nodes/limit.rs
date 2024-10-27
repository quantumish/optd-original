use super::{
    macros::define_plan_node, ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfReprPlanNode,
};

#[derive(Clone, Debug)]
pub struct LogicalLimit(pub ArcDfPlanNode);

define_plan_node!(
    LogicalLimit : DfPlanNode,
    Limit, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, skip: ArcDfPredNode },
        { 1, fetch: ArcDfPredNode }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalLimit(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalLimit : DfPlanNode,
    PhysicalLimit, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, skip: ArcDfPredNode },
        { 1, fetch: ArcDfPredNode }
    ]
);
