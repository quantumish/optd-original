//! This rule is designed to be applied heuristically (read: all the time, blindly).
//! However, pushing a filter is not *always* better (but it usually is). If cost is
//! to be taken into account, each transposition step can be done separately
//! (and are thus all in independent functions).
//! One can even implement each of these helper functions as their own transpose rule,
//! like Calcite does.
//!
//! At a high level, filter pushdown is responsible for pushing the filter node
//! further down the query plan whenever it is possible to do so.

use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};
use tracing_subscriber::filter::combinator::And;

use crate::plan_nodes::{
    BinOpExpr, BinOpType, Expr, ExprList, LogOpExpr, LogOpType, LogicalFilter, LogicalJoin,
    LogicalProjection, LogicalSort, OptRelNode, OptRelNodeTyp,
};
use crate::properties::schema::SchemaPropertyBuilder;

use super::macros::define_rule;

define_rule!(
    FilterPushdownRule,
    apply_filter_pushdown,
    (Filter, child, [cond])
);

fn merge_conds(first: Expr, second: Expr) -> Expr {
    let new_expr_list = ExprList::new(vec![first, second]);
    // Flatten nested logical expressions if possible
    LogOpExpr::new_flattened_nested_logical(LogOpType::And, new_expr_list).into_expr()
}

/// Datafusion only pushes filter past project when the project does not contain
/// volatile (i.e. non-deterministic) expressions that are present in the filter
/// Calcite only checks if the projection contains a windowing calculation
/// We check neither of those things and do it always (which may be wrong)
fn filter_project_transpose(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    child: RelNode<OptRelNodeTyp>,
    cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let old_proj = LogicalProjection::from_rel_node(child.into()).unwrap();
    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();

    // TODO: Implement get_property in heuristics optimizer
    let projection_schema_len = optimizer
        .get_property::<SchemaPropertyBuilder>(old_proj.clone().into_rel_node(), 0)
        .len();
    let child_schema_len = optimizer
        .get_property::<SchemaPropertyBuilder>(old_proj.clone().into_rel_node(), 0)
        .len();

    let proj_col_map = old_proj.compute_column_mapping().unwrap();
    proj_col_map.rewrite_condition(
        cond_as_expr.clone(),
        projection_schema_len,
        child_schema_len,
    );

    let new_filter_node = LogicalFilter::new(old_proj.child(), cond_as_expr);
    let new_proj = LogicalProjection::new(new_filter_node.into_plan_node(), old_proj.exprs());
    vec![new_proj.into_rel_node().as_ref().clone()]
}

fn filter_merge(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    child: RelNode<OptRelNodeTyp>,
    cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child_filter = LogicalFilter::from_rel_node(child.into()).unwrap();
    let child_filter_cond = child_filter.cond().clone();
    let curr_cond = Expr::from_rel_node(cond.into()).unwrap();
    let merged_cond = merge_conds(curr_cond, child_filter_cond);
    let new_filter = LogicalFilter::new(child_filter.child(), merged_cond);
    vec![new_filter.into_rel_node().as_ref().clone()]
}

/// Cases:
/// - Push down to the left child (only involves keys from the left child)
/// - Push down to the right child (only involves keys from the right child)
/// - Push into the join condition (involves keys from both children)
fn filter_join_transpose(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    child: RelNode<OptRelNodeTyp>,
    cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let _old_join = LogicalJoin::from_rel_node(child.into()).unwrap();
    let _cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();

    vec![]
}

/// Filter and sort should always be commutable.
fn filter_sort_transpose(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    child: RelNode<OptRelNodeTyp>,
    cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let old_sort = LogicalSort::from_rel_node(child.into()).unwrap();
    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();
    let new_filter_node = LogicalFilter::new(old_sort.child(), cond_as_expr);
    // Exprs should be the same, no projections have occurred here.
    let new_sort = LogicalSort::new(new_filter_node.into_plan_node(), old_sort.exprs());
    vec![new_sort.into_rel_node().as_ref().clone()]
}

fn apply_filter_pushdown(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterPushdownRulePicks { child, cond }: FilterPushdownRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // Push filter down one node
    let mut result_from_this_step = match child.typ {
        OptRelNodeTyp::Projection => filter_project_transpose(optimizer, child, cond),
        OptRelNodeTyp::Filter => filter_merge(optimizer, child, cond),
        // OptRelNodeTyp::Scan => todo!(),   // TODO: Add predicate field to scan node
        OptRelNodeTyp::Join(_) => filter_join_transpose(optimizer, child, cond),
        OptRelNodeTyp::Sort => filter_sort_transpose(optimizer, child, cond),
        _ => vec![],
    };

    // Apply rule recursively
    if let Some(new_node) = result_from_this_step.first_mut() {
        // For all the children in our result,
        for child in new_node.children.iter_mut() {
            if child.typ == OptRelNodeTyp::Filter {
                // If this node is a filter, apply the rule again to this node!
                let child_as_filter = LogicalFilter::from_rel_node(child.clone()).unwrap();
                let childs_child = child_as_filter.child().into_rel_node().as_ref().clone();
                let childs_cond = child_as_filter.cond().into_rel_node().as_ref().clone();
                // @todo: make this iterative?
                let result = apply_filter_pushdown(
                    optimizer,
                    FilterPushdownRulePicks {
                        child: childs_child,
                        cond: childs_cond,
                    },
                );
                // If we got a result, that is the replacement for this child
                if let Some(&new_child) = result.first().as_ref() {
                    *child = new_child.to_owned().into();
                }
            }
            // Otherwise, if there was no result from rule application or this is not a filter, do not modify the child
        }
    }

    result_from_this_step
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::compute::kernels::filter;
    use optd_core::heuristics::{ApplyOrder, HeuristicsOptimizer};

    use crate::plan_nodes::{
        BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ExprList, LogOpExpr, LogOpType,
        LogicalFilter, LogicalProjection, LogicalScan, LogicalSort, OptRelNode, OptRelNodeTyp,
    };

    use super::apply_filter_pushdown;

    #[test]
    fn push_past_sort() {
        let dummy_optimizer = HeuristicsOptimizer::new_with_rules(vec![], ApplyOrder::TopDown);

        let scan = LogicalScan::new("".into());
        let sort = LogicalSort::new(scan.into_plan_node(), ExprList::new(vec![]));

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let plan = apply_filter_pushdown(
            &dummy_optimizer,
            super::FilterPushdownRulePicks {
                child: Arc::unwrap_or_clone(sort.into_rel_node()),
                cond: Arc::unwrap_or_clone(filter_expr.into_rel_node()),
            },
        );

        let plan = plan.first().unwrap();

        assert!(matches!(plan.typ, OptRelNodeTyp::Sort));
        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Filter));
    }

    #[test]
    fn filter_merge() {
        // TODO: write advanced proj with more expr that need to be transformed
        let dummy_optimizer = HeuristicsOptimizer::new_with_rules(vec![], ApplyOrder::TopDown);

        let scan = LogicalScan::new("".into());
        let filter_ch_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(1).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();
        let filter_ch = LogicalFilter::new(scan.into_plan_node(), filter_ch_expr);

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(1).into_expr(),
            ConstantExpr::int32(6).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let plan = apply_filter_pushdown(
            &dummy_optimizer,
            super::FilterPushdownRulePicks {
                child: Arc::unwrap_or_clone(filter_ch.into_rel_node()),
                cond: Arc::unwrap_or_clone(filter_expr.into_rel_node()),
            },
        );

        let plan = plan.first().unwrap();

        assert!(matches!(plan.typ, OptRelNodeTyp::Filter));
        let cond_log_op = LogOpExpr::from_rel_node(
            LogicalFilter::from_rel_node((plan.clone()).into())
                .unwrap()
                .cond()
                .into_rel_node(),
        )
        .unwrap();
        assert!(matches!(cond_log_op.op_type(), LogOpType::And));

        assert!(matches!(
            cond_log_op.child(0).typ(),
            OptRelNodeTyp::ColumnRef
        ));
        let col_rel_0 = ColumnRefExpr::from_rel_node(cond_log_op.child(0).into_rel_node()).unwrap();
        assert_eq!(col_rel_0.index(), 0);

        assert!(matches!(
            cond_log_op.child(1).typ(),
            OptRelNodeTyp::Constant(_)
        ));
        let col_rel_1 = ConstantExpr::from_rel_node(cond_log_op.child(1).into_rel_node()).unwrap();
        assert_eq!(col_rel_1.value().as_i32(), 1);

        assert!(matches!(
            cond_log_op.child(2).typ(),
            OptRelNodeTyp::ColumnRef
        ));
        let col_rel_2 = ColumnRefExpr::from_rel_node(cond_log_op.child(2).into_rel_node()).unwrap();
        assert_eq!(col_rel_2.index(), 1);

        assert!(matches!(
            cond_log_op.child(3).typ(),
            OptRelNodeTyp::Constant(_)
        ));
        let col_rel_3 = ConstantExpr::from_rel_node(cond_log_op.child(3).into_rel_node()).unwrap();
        assert_eq!(col_rel_3.value().as_i32(), 6);
    }

    #[test]
    fn push_past_proj_basic() {
        // TODO: write advanced proj with more expr that need to be transformed
        let dummy_optimizer = HeuristicsOptimizer::new_with_rules(vec![], ApplyOrder::TopDown);

        let scan = LogicalScan::new("".into());
        let proj = LogicalProjection::new(scan.into_plan_node(), ExprList::new(vec![]));

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let plan = apply_filter_pushdown(
            &dummy_optimizer,
            super::FilterPushdownRulePicks {
                child: Arc::unwrap_or_clone(proj.into_rel_node()),
                cond: Arc::unwrap_or_clone(filter_expr.into_rel_node()),
            },
        );

        let plan = plan.first().unwrap();

        assert!(matches!(plan.typ, OptRelNodeTyp::Projection));
        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Filter));
    }
}
