use optd_core::nodes::{PlanNode, PlanNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use super::macros::define_plan_node;
use super::{
    ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfPlanNodeOrGroup, DfReprPlanNode,
    JoinType,
};

#[derive(Clone, Debug)]
pub struct RawDependentJoin(pub ArcDfPlanNode);

define_plan_node!(
    RawDependentJoin : DfReprPlanNode,
    RawDepJoin, [
        { 0, left: DfPlanNodeOrGroup },
        { 1, right: DfPlanNodeOrGroup }
    ], [
        { 0, cond: ArcDfPredNode },
        { 1, extern_cols: ArcDfPredNode }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct DependentJoin(pub ArcDfPlanNode);

define_plan_node!(
    DependentJoin : DfReprPlanNode,
    DepJoin, [
        { 0, left: DfPlanNodeOrGroup },
        { 1, right: DfPlanNodeOrGroup }
    ], [
        { 0, cond: ArcDfPredNode },
        { 1, extern_cols: ArcDfPredNode }
    ], { join_type: JoinType }
);
