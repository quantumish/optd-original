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

use crate::plan_nodes::{
    Expr, LogicalFilter, LogicalProjection, LogicalSort, OptRelNode, OptRelNodeTyp,
};

use super::macros::define_rule;

define_rule!(
    FilterPushdownRule,
    apply_filter_pushdown,
    (Filter, child, [cond])
);

/// Datafusion only pushes filter past project when the project does not contain
/// volatile (i.e. non-deterministic) expressions that are present in the filter
/// Calcite only checks if the projection contains a windowing calculation
fn filter_project_transpose(
    child: RelNode<OptRelNodeTyp>,
    cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let old_proj = LogicalProjection::from_rel_node(child.into()).unwrap();
    vec![]
}

/// Filter and sort should always be commutable.
fn filter_sort_transpose(
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
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterPushdownRulePicks { child, cond }: FilterPushdownRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // Push filter down one node
    let mut result_from_this_step = match child.typ {
        OptRelNodeTyp::Projection => filter_project_transpose(child, cond),
        OptRelNodeTyp::Filter => todo!(), // @todo filter merge rule? Should we do that here?
        // OptRelNodeTyp::Scan => todo!(),   // TODO: Add predicate field to scan node
        OptRelNodeTyp::Join(_) => todo!(),
        OptRelNodeTyp::Sort => filter_sort_transpose(child, cond),
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
                    _optimizer,
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
    use std::{any::Any, sync::Arc};

    use optd_core::heuristics::{ApplyOrder, HeuristicsOptimizer};

    use crate::plan_nodes::{
        BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ExprList, LogicalFilter, LogicalScan,
        LogicalSort, OptRelNode, OptRelNodeTyp,
    };

    use super::apply_filter_pushdown;

    #[test]
    fn filter_before_sort() {
        let dummy_optimizer = HeuristicsOptimizer::new_with_rules(vec![], ApplyOrder::TopDown);

        let scan = LogicalScan::new("".into());
        let sort = LogicalSort::new(scan.into_plan_node(), ExprList::new(vec![]));

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();
        let filter = LogicalFilter::new(sort.clone().into_plan_node(), filter_expr.clone());

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
}
