use super::macros::define_plan_node;

use super::{
    ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfPlanNodeOrGroup, DfReprPlanNode,
};

use crate::plan_nodes::{dispatch_plan_explain, dispatch_pred_explain, get_meta};

#[derive(Clone, Debug)]
pub struct LogicalFilter(pub ArcDfPlanNode);

define_plan_node!(
    LogicalFilter : DfPlanNode,
    Filter, [
        { 0, child: DfPlanNodeOrGroup }
    ], [
        { 0, cond: ArcDfPredNode }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalFilter(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalFilter : DfPlanNode,
    PhysicalFilter, [
        { 0, child: DfPlanNodeOrGroup }
    ], [
        { 0, cond: ArcDfPredNode }
    ]
);
