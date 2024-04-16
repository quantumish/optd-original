use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    ExprList, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};
use crate::rules::macros::define_rule;

use super::project_transpose_common::ProjectionMapping;
// test cases for project merge
// create table t1 (v1 int, v2 int);
// explain select v1,v2 from (select v1,v2 from t1);

// create table t3 (v1 int, v2 int, v3 int);
// explain select v2,v3 from (select v1,v3,v2 from t3);
// explain select v1,v2,v3 from (select v1,v3,v2 from t3);

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

    // println!("res_exprs: {:?}\n exprs1: {:?}\n child: {:?}\n exprs2: {:?}\n", res_exprs, exprs1, child, exprs2);

    let node: LogicalProjection = LogicalProjection::new(
        child,
        res_exprs,
    );

    vec![node.into_rel_node().as_ref().clone()]
}
