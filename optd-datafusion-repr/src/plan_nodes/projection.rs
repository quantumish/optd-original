use super::macros::define_plan_node;
use super::predicates::ListPred;

use super::{
    ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfPlanNodeOrGroup, DfReprPlanNode,
};

#[derive(Clone, Debug)]
pub struct LogicalProjection(pub ArcDfPlanNode);

define_plan_node!(
    LogicalProjection : DfPlanNode,
    Projection, [
        { 0, child: DfPlanNodeOrGroup }
    ], [
        { 0, exprs: ArcDfPredNode }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalProjection(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalProjection : DfPlanNode,
    PhysicalProjection, [
        { 0, child: DfPlanNodeOrGroup }
    ], [
        { 0, exprs: ArcDfPredNode }
    ]
);
