use super::{
    macros::define_plan_node, ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode,
    DfPlanNodeOrGroup, DfReprPlanNode,
};

use crate::plan_nodes::{dispatch_plan_explain, dispatch_pred_explain, get_meta};

#[derive(Clone, Debug)]
pub struct LogicalLimit(pub ArcDfPlanNode);

define_plan_node!(
    LogicalLimit : DfPlanNode,
    Limit, [
        { 0, child: DfPlanNodeOrGroup }
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
        { 0, child: DfPlanNodeOrGroup }
    ], [
        { 0, skip: ArcDfPredNode },
        { 1, fetch: ArcDfPredNode }
    ]
);
