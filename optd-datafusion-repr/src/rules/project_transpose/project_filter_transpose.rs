use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    Expr, ExprList, LogicalFilter, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};
use crate::rules::macros::define_rule;
use super::project_transpose_common::ProjectionMapping;

fn merge_exprs(first: ExprList, second: ExprList) -> ExprList {
    let mut res_vec = first.to_vec();
    res_vec.extend(second.to_vec());
    ExprList::new(res_vec)
}

// pushes projections through filters
// adds a projection node after a filter node 
// only keeping necessary columns (proj node exprs + filter col exprs)
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
    let Some(mapping) = ProjectionMapping::build(&bottom_proj_exprs) else {
        return vec![];
    };

    let child = PlanNode::from_group(child.into());
    let new_filter_cond: Expr = mapping.rewrite_filter_cond(cond_as_expr.clone(), true);
    let bottom_proj_node = LogicalProjection::new(child, bottom_proj_exprs);
    let new_filter_node = LogicalFilter::new(bottom_proj_node.into_plan_node(), new_filter_cond);

    if dedup_cond_col_refs.is_empty() {
        // can push proj past filter and remove top proj node
        return vec![new_filter_node.into_rel_node().as_ref().clone()];
    }    
    
    // have column ref expressions of cond cols
    // bottom-most projection will have proj cols + filter cols as a set
    let Some(top_proj_exprs) = mapping.rewrite_projection(&exprs, false) else {
        return vec![];
    };
    let top_proj_node = LogicalProjection::new(new_filter_node.into_plan_node(), top_proj_exprs);
    vec![top_proj_node.into_rel_node().as_ref().clone()]
}