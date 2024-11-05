use optd_core::nodes::PlanNodeOrGroup;

use super::macros::define_plan_node;
use super::predicates::ListPred;

use super::{
    ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfPlanNodeOrGroup, DfReprPlanNode,
};

use crate::plan_nodes::{dispatch_plan_explain, dispatch_pred_explain, get_meta};

#[derive(Clone, Debug)]
pub struct LogicalAgg(pub ArcDfPlanNode);

define_plan_node!(
    LogicalAgg : DfPlanNode,
    Agg, [
        { 0, child: DfPlanNodeOrGroup }
    ], [
        { 0, exprs: ArcDfPredNode },
        { 1, groups: ArcDfPredNode }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalAgg(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalAgg : DfPlanNode,
    PhysicalAgg, [
        { 0, child:  DfPlanNodeOrGroup}
    ], [
        { 0, aggrs: ArcDfPredNode },
        { 1, groups: ArcDfPredNode }
    ]
);
