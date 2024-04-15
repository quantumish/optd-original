use std::collections::HashMap;
use std::vec;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    Expr, ExprList, LogicalFilter, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};

use crate::rules::macros::define_rule;
use crate::rules::project_transpose::project_transpose_common::ProjectionMapping;

define_rule!(
    FilterProjectTransposeRule,
    apply_filter_project_transpose,
    (Filter, (Projection, child, [exprs]), [cond])
);

/// Datafusion only pushes filter past project when the project does not contain
/// volatile (i.e. non-deterministic) expressions that are present in the filter
/// Calcite only checks if the projection contains a windowing calculation
/// We check neither of those things and do it always (which may be wrong)
fn apply_filter_project_transpose(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterProjectTransposeRulePicks { child, exprs, cond }: FilterProjectTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child = PlanNode::from_group(child.into());
    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();
    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();

    let proj_col_map = ProjectionMapping::build(&exprs).unwrap();
    let rewritten_cond = proj_col_map.rewrite_filter_cond(cond_as_expr.clone(), false);

    let new_filter_node = LogicalFilter::new(child, rewritten_cond);
    let new_proj = LogicalProjection::new(new_filter_node.into_plan_node(), exprs);
    vec![new_proj.into_rel_node().as_ref().clone()]
}