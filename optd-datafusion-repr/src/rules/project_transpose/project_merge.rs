use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    ExprList, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};
use crate::rules::macros::define_rule;

use super::project_transpose_common::ProjectionMapping;

// Proj (Proj A) -> Proj A
// merges/removes projections
define_rule!(
    ProjectMergeRule,
    apply_projection_merge,
    (
        Projection, 
        (Projection, child, [exprs2]), 
        [exprs1]
    )
);

fn apply_projection_merge(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectMergeRulePicks { child, exprs1, exprs2 }: ProjectMergeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child = PlanNode::from_group(child.into());
    let exprs1 = ExprList::from_rel_node(exprs1.into()).unwrap();
    let exprs2 = ExprList::from_rel_node(exprs2.into()).unwrap();

    let Some(mapping) = ProjectionMapping::build(&exprs1) else {
        return vec![];
    };

    let Some(res_exprs) = mapping.rewrite_projection(&exprs2, true) else {
        return vec![];
    };

    let node: LogicalProjection = LogicalProjection::new(
        child,
        res_exprs,
    );

    vec![node.into_rel_node().as_ref().clone()]
}
