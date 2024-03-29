use std::collections::HashMap;
use std::sync::Arc;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    BetweenExpr, ColumnRefExpr, Expr, ExprList, LikeExpr, LogOpExpr, LogicalFilter, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};
use crate::properties::column_ref::ColumnRef;
use crate::properties::schema::SchemaPropertyBuilder;

use super::macros::define_rule;

fn merge_exprs(first: ExprList, second: ExprList) -> ExprList {
    let mut res_vec = first.to_vec();
    res_vec.extend(second.to_vec());
    ExprList::new(res_vec)
}

// projects away aggregate calls that are not used
// TODO
define_rule!(
    ProjectAggregatePushDown,
    apply_projection_agg_pushdown,
    (
        Projection, 
        (Agg, child, [agg_exprs], [agg_groups]), 
        [exprs]
    )
);

fn apply_projection_agg_pushdown(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectAggregatePushDownPicks { child, agg_exprs, agg_groups, exprs }: ProjectAggregatePushDownPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {



    vec![]
}

// pushes projections through filters
// adds a projection node after a filter node 
// only keeping necessary columns (proj node exprs + filter col exprs))
// TODO
define_rule!(
    ProjectFilterTransposeRule,
    apply_projection_filter_transpose,
    (
        Projection, 
        (Filter, child, [cond]), 
        [exprs]
    )
);

fn apply_projection_filter_transpose(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectFilterTransposeRulePicks { child, cond, exprs }: ProjectFilterTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // get columns out of cond
    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();
    let exprs_vec = exprs.clone().to_vec();
    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();
    let cond_col_refs = cond_as_expr.get_column_refs();
    let mut dedup_cond_col_refs = Vec::new();

    for i in 0..cond_col_refs.len() {
        if !exprs_vec.contains(&cond_col_refs[i]) {
            dedup_cond_col_refs.push(cond_col_refs[i].clone());
        };
    };

    let dedup_cond_col_refs = ExprList::new(dedup_cond_col_refs);

    let bottom_proj_exprs: ExprList = merge_exprs(exprs.clone(), dedup_cond_col_refs.clone());
    let Some(mapping) = LogicalProjection::compute_column_mapping(&bottom_proj_exprs) else {
        return vec![];
    };

    let child_schema_len = _optimizer
        .get_property::<SchemaPropertyBuilder>(child.clone().into(), 0)
        .len();
    let child = PlanNode::from_group(child.into());
    let new_filter_cond: Expr = mapping.rewrite_condition(cond_as_expr.clone(), child_schema_len);
    let bottom_proj_node = LogicalProjection::new(child, bottom_proj_exprs);
    let new_filter_node = LogicalFilter::new(bottom_proj_node.into_plan_node(), new_filter_cond);

    if dedup_cond_col_refs.is_empty() {
        // can push proj past filter and remove top proj node
        return vec![new_filter_node.into_rel_node().as_ref().clone()];
    }    
    
    // have column ref expressions of cond cols
    // bottom-most projection will have proj cols + filter cols as a set
    let top_proj_exprs = mapping.reverse_rewrite_projection(&exprs);
    let top_proj_node = LogicalProjection::new(new_filter_node.into_plan_node(), top_proj_exprs);
    vec![top_proj_node.into_rel_node().as_ref().clone()]
}


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

    let Some(mapping) = LogicalProjection::compute_column_mapping(&exprs1) else {
        return vec![];
    };

    let res_exprs = mapping.rewrite_projection(&exprs2);

    let node: LogicalProjection = LogicalProjection::new(
        child,
        res_exprs,
    );
    vec![node.into_rel_node().as_ref().clone()]
}
