//! This rule is designed to be applied heuristically (read: all the time, blindly).
//! However, pushing a filter is not *always* better (but it usually is). If cost is
//! to be taken into account, each transposition step can be done separately
//! (and are thus all in independent functions).
//! One can even implement each of these helper functions as their own transpose rule,
//! like Calcite does.
//!
//! At a high level, filter pushdown is responsible for pushing the filter node
//! further down the query plan whenever it is possible to do so.

use core::panic;
use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    BinOpExpr, ColumnRefExpr, Expr, ExprList, JoinType, LogOpExpr, LogOpType, LogicalFilter,
    LogicalJoin, LogicalProjection, LogicalSort, MappedColRef, OptRelNode, OptRelNodeTyp,
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

// Recursively search through all predicates in the join condition (LogExprs and BinOps),
// separating them into those that only involve the left child, those that only involve the
// right child, and those that involve both children. Constant expressions involve neither
// child.
fn separate_join_conds(
    cond: LogOpExpr,
    left_schema_size: usize,
    right_schema_size: usize,
) -> (Vec<Expr>, Vec<Expr>, Vec<Expr>, Vec<Expr>) {
    let mut left_conds = vec![];
    let mut right_conds = vec![];
    let mut join_conds = vec![];
    let mut keep_conds = vec![];

    // For each child, if it is a LogOpExpr, recursively call this function
    // If it is a BinOpExpr, check both children and add to the appropriate list
    // If this is an AND logopexpr, then each of the conditions can be separated.
    // If this is an OR logopexpr, then we have to check if that entire logopexpr
    // can be separated.
    for child in cond.children() {
        match child.typ() {
            OptRelNodeTyp::LogOp(LogOpType::And) => {
                let log_expr = LogOpExpr::from_rel_node(child.into_rel_node()).unwrap();
                // Recurse
                let (left, right, join, keep) =
                    separate_join_conds(log_expr.clone(), left_schema_size, right_schema_size);
                left_conds.extend(left);
                right_conds.extend(right);
                join_conds.extend(join);
                keep_conds.extend(keep);
            }
            OptRelNodeTyp::LogOp(LogOpType::Or) => {
                todo!("LogOpTyp::Or not yet implemented---God help us all")
            }
            OptRelNodeTyp::BinOp(_) => {
                let bin_expr = BinOpExpr::from_rel_node(child.into_rel_node()).unwrap();
                // Check if the left and right children are column refs
                let left_col = bin_expr.left_child();
                let right_col = bin_expr.right_child();
                let left_col = match left_col.typ() {
                    OptRelNodeTyp::ColumnRef => Some(LogicalJoin::map_through_join(
                        ColumnRefExpr::from_rel_node(left_col.into_rel_node())
                            .unwrap()
                            .index(),
                        left_schema_size,
                        right_schema_size,
                    )),
                    _ => None,
                };
                let right_col = match right_col.typ() {
                    OptRelNodeTyp::ColumnRef => Some(LogicalJoin::map_through_join(
                        ColumnRefExpr::from_rel_node(right_col.into_rel_node())
                            .unwrap()
                            .index(),
                        left_schema_size,
                        right_schema_size,
                    )),
                    _ => None,
                };
                // Check if cols list contains only left, only right, a mix, or is empty
                // Note that the left col and right col can both be on the right side or left side
                // of the join, so we need to check both
                match (left_col, right_col) {
                    (Some(MappedColRef::Left(_)), Some(MappedColRef::Left(_))) => {
                        left_conds.push(bin_expr.clone().into_expr());
                    }
                    (Some(MappedColRef::Right(_)), Some(MappedColRef::Right(_))) => {
                        right_conds.push(bin_expr.clone().into_expr());
                    }
                    (Some(MappedColRef::Left(_)), Some(MappedColRef::Right(_)))
                    | (Some(MappedColRef::Right(_)), Some(MappedColRef::Left(_))) => {
                        join_conds.push(bin_expr.clone().into_expr());
                    }
                    _ => {
                        // If †his is a constant expression, another rule should
                        // handle it. We won't push it down.
                        keep_conds.push(bin_expr.clone().into_expr());
                    }
                }
            }
            _ => {
                panic!("Expression type {} not yet implemented", child.typ())
            }
        }
    }

    (left_conds, right_conds, join_conds, keep_conds)
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
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    child: RelNode<OptRelNodeTyp>,
    cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let old_join = LogicalJoin::from_rel_node(child.into()).unwrap();
    let cond_as_logexpr = LogOpExpr::from_rel_node(cond.into()).unwrap();

    let left_schema_size = optimizer
        .get_property::<SchemaPropertyBuilder>(old_join.left().into_rel_node(), 0)
        .len();
    let right_schema_size = optimizer
        .get_property::<SchemaPropertyBuilder>(old_join.right().into_rel_node(), 0)
        .len();

    let (left_conds, right_conds, join_conds, keep_conds) =
        separate_join_conds(cond_as_logexpr, left_schema_size, right_schema_size);

    let new_left = if !left_conds.is_empty() {
        let new_filter_node = LogicalFilter::new(
            old_join.left(),
            LogOpExpr::new(LogOpType::And, ExprList::new(left_conds)).into_expr(),
        );
        new_filter_node.into_plan_node()
    } else {
        old_join.left()
    };

    let new_right = if !right_conds.is_empty() {
        let new_filter_node = LogicalFilter::new(
            old_join.right(),
            LogOpExpr::new(LogOpType::And, ExprList::new(right_conds)).into_expr(),
        );
        new_filter_node.into_plan_node()
    } else {
        old_join.right()
    };

    let new_join = match old_join.join_type() {
        JoinType::Inner => {
            let old_cond = old_join.cond();
            let new_conds = merge_conds(
                LogOpExpr::new(LogOpType::And, ExprList::new(join_conds)).into_expr(),
                old_cond,
            );
            LogicalJoin::new(new_left, new_right, new_conds, JoinType::Inner)
        }
        JoinType::Cross => {
            if !join_conds.is_empty() {
                LogicalJoin::new(
                    new_left,
                    new_right,
                    LogOpExpr::new(LogOpType::And, ExprList::new(join_conds)).into_expr(),
                    JoinType::Inner,
                )
            } else {
                LogicalJoin::new(new_left, new_right, old_join.cond(), JoinType::Cross)
            }
        }
        _ => {
            // We don't support modifying the join condition for other join types
            LogicalJoin::new(new_left, new_right, old_join.cond(), old_join.join_type())
        }
    };

    let new_node = if !keep_conds.is_empty() {
        let new_filter_node = LogicalFilter::new(
            new_join.into_plan_node(),
            LogOpExpr::new(LogOpType::And, ExprList::new(keep_conds)).into_expr(),
        );
        new_filter_node.into_rel_node().as_ref().clone()
    } else {
        new_join.into_rel_node().as_ref().clone()
    };

    vec![new_node]
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

    use crate::{
        plan_nodes::{
            BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ExprList, LogOpExpr, LogOpType,
            LogicalFilter, LogicalProjection, LogicalScan, LogicalSort, OptRelNode, OptRelNodeTyp,
        },
        testing::new_dummy_optimizer,
    };

    use super::apply_filter_pushdown;

    #[test]
    fn push_past_sort() {
        let dummy_optimizer = new_dummy_optimizer();

        let scan = LogicalScan::new("customer".into());
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
        let dummy_optimizer = new_dummy_optimizer();

        let scan = LogicalScan::new("customer".into());
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

        let cond_exprs = cond_log_op.children();
        assert_eq!(cond_exprs.len(), 2);
        let expr_1 = BinOpExpr::from_rel_node(cond_exprs[0].clone().into_rel_node()).unwrap();
        let expr_2 = BinOpExpr::from_rel_node(cond_exprs[1].clone().into_rel_node()).unwrap();
        assert!(matches!(expr_1.op_type(), BinOpType::Eq));
        assert!(matches!(expr_2.op_type(), BinOpType::Eq));
        let col_1 =
            ColumnRefExpr::from_rel_node(expr_1.left_child().clone().into_rel_node()).unwrap();
        let col_2 =
            ConstantExpr::from_rel_node(expr_1.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_1.index(), 1);
        assert_eq!(col_2.value().as_i32(), 6);
        let col_3 =
            ColumnRefExpr::from_rel_node(expr_2.left_child().clone().into_rel_node()).unwrap();
        let col_4 =
            ConstantExpr::from_rel_node(expr_2.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_3.index(), 0);
        assert_eq!(col_4.value().as_i32(), 1);
    }

    #[test]
    fn push_past_proj_basic() {
        // TODO: write advanced proj with more expr that need to be transformed
        let dummy_optimizer = new_dummy_optimizer();

        let scan = LogicalScan::new("customer".into());
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
