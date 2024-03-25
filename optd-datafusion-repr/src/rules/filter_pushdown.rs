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
    LogicalJoin, LogicalProjection, LogicalSort, OptRelNode, OptRelNodeTyp,
};
use crate::properties::schema::SchemaPropertyBuilder;

use super::macros::define_rule;

define_rule!(
    FilterPushdownRule,
    apply_filter_pushdown,
    (Filter, [child], [cond])
);

fn merge_conds(first: Expr, second: Expr) -> Expr {
    let new_expr_list = ExprList::new(vec![first, second]);
    // Flatten nested logical expressions if possible
    LogOpExpr::new_flattened_nested_logical(LogOpType::And, new_expr_list).into_expr()
}

#[derive(Debug, Clone, Copy)]
enum JoinCondDependency {
    Left,
    Right,
    Both,
    None,
}

fn determine_join_cond_dep(
    children: Vec<Expr>,
    left_schema_size: usize,
    right_schema_size: usize,
) -> JoinCondDependency {
    let mut left_col = false;
    let mut right_col = false;
    for child in children {
        match child.typ() {
            OptRelNodeTyp::ColumnRef => {
                let col_ref = ColumnRefExpr::from_rel_node(child.into_rel_node()).unwrap();
                let index = col_ref.index();
                if index < left_schema_size {
                    left_col = true;
                } else if index >= left_schema_size && index < left_schema_size + right_schema_size
                {
                    right_col = true;
                }
            }
            _ => {}
        }
    }
    match (left_col, right_col) {
        (true, true) => JoinCondDependency::Both,
        (true, false) => JoinCondDependency::Left,
        (false, true) => JoinCondDependency::Right,
        (false, false) => JoinCondDependency::None,
    }
}

// Recursively search through all predicates in the join condition (LogExprs and BinOps),
// separating them into those that only involve the left child, those that only involve the
// right child, and those that involve both children. Constant expressions involve neither
// child.
// pre-condition: the cond is an AND LogOpExpr
fn separate_join_conds(
    cond: LogOpExpr,
    left_schema_size: usize,
    right_schema_size: usize,
) -> (Vec<Expr>, Vec<Expr>, Vec<Expr>, Vec<Expr>) {
    let mut left_conds = vec![];
    let mut right_conds = vec![];
    let mut join_conds = vec![];
    let mut keep_conds = vec![];

    // For each child, if it is a LogOpExpr with and, recursively call this function
    // If it is a BinOpExpr, check both children and add to the appropriate list
    // If this is an AND logopexpr, then each of the conditions can be separated.
    // If this is an OR logopexpr, then we have to check if that entire logopexpr
    // can be separated.
    for child in cond.children() {
        dbg!("Working on child: ", child.clone());
        let location = match child.typ() {
            OptRelNodeTyp::LogOp(LogOpType::And) => {
                // In theory, we could recursively call the function to handle this
                // case. However, it should not be possible to have nested LogOpExpr
                // ANDs. We panic so that we can notice the bug.
                panic!("Nested AND LogOpExprs detected in filter pushdown!");
            }
            OptRelNodeTyp::LogOp(LogOpType::Or) => {
                let log_expr = LogOpExpr::from_rel_node(child.clone().into_rel_node()).unwrap();
                determine_join_cond_dep(log_expr.children(), left_schema_size, right_schema_size)
            }
            OptRelNodeTyp::BinOp(_) => {
                let bin_expr = BinOpExpr::from_rel_node(child.clone().into_rel_node()).unwrap();
                determine_join_cond_dep(
                    vec![bin_expr.left_child(), bin_expr.right_child()],
                    left_schema_size,
                    right_schema_size,
                )
            }
            _ => {
                panic!(
                    "Expression type {} not yet implemented for separate_join_conds",
                    child.typ()
                )
            }
        };

        println!("Location: {:?}", location.clone());
        match location {
            JoinCondDependency::Left => left_conds.push(child),
            JoinCondDependency::Right => right_conds.push(child.rewrite_column_refs(&|idx| {
                LogicalJoin::map_through_join(idx, left_schema_size, right_schema_size)
            })),
            JoinCondDependency::Both => join_conds.push(child),
            JoinCondDependency::None => keep_conds.push(child),
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
    let rewritten_cond = proj_col_map.rewrite_condition(
        cond_as_expr.clone(),
        projection_schema_len,
        child_schema_len,
    );

    let new_filter_node = LogicalFilter::new(old_proj.child(), rewritten_cond);
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
    println!("Filter join transpose hit with type {:?}", child.typ);
    // TODO: Push existing join conditions down as well
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
            // We don't support modifying the join condition for other join types yet
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
            LogicalFilter, LogicalJoin, LogicalProjection, LogicalScan, LogicalSort, OptRelNode,
            OptRelNodeTyp,
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
        let mut dummy_optimizer = new_dummy_optimizer();

        let scan = LogicalScan::new("customer".into());
        let proj = LogicalProjection::new(scan.into_plan_node(), ExprList::new(vec![]));

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        // Initialize groups
        dummy_optimizer
            .step_optimize_rel(proj.clone().into_rel_node())
            .unwrap();

        dummy_optimizer
            .step_optimize_rel(proj.clone().child().into_rel_node())
            .unwrap();

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

    #[test]
    fn push_past_proj_adv() {
        let mut dummy_optimizer = new_dummy_optimizer();

        let scan = LogicalScan::new("customer".into());
        let proj = LogicalProjection::new(
            scan.into_plan_node(),
            ExprList::new(vec![
                ColumnRefExpr::new(0).into_expr(),
                ColumnRefExpr::new(4).into_expr(),
                ColumnRefExpr::new(5).into_expr(),
                ColumnRefExpr::new(7).into_expr(),
            ]),
        );

        let filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    // This one should be pushed to the left child
                    ColumnRefExpr::new(1).into_expr(),
                    ConstantExpr::int32(5).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // This one should be pushed to the right child
                    ColumnRefExpr::new(3).into_expr(),
                    ConstantExpr::int32(6).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        );

        // Initialize groups
        dummy_optimizer
            .step_optimize_rel(proj.clone().into_rel_node())
            .unwrap();

        dummy_optimizer
            .step_optimize_rel(proj.clone().child().into_rel_node())
            .unwrap();

        let plan = apply_filter_pushdown(
            &dummy_optimizer,
            super::FilterPushdownRulePicks {
                child: Arc::unwrap_or_clone(proj.into_rel_node()),
                cond: Arc::unwrap_or_clone(filter_expr.into_rel_node()),
            },
        );

        let plan = plan.first().unwrap();

        assert!(matches!(plan.typ, OptRelNodeTyp::Projection));
        let plan_filter = LogicalFilter::from_rel_node(plan.child(0)).unwrap();
        assert!(matches!(plan_filter.0.typ(), OptRelNodeTyp::Filter));
        let plan_filter_expr =
            LogOpExpr::from_rel_node(plan_filter.cond().into_rel_node()).unwrap();
        assert!(matches!(plan_filter_expr.op_type(), LogOpType::And));
        let op_0 = BinOpExpr::from_rel_node(plan_filter_expr.children()[0].clone().into_rel_node())
            .unwrap();
        let col_0 =
            ColumnRefExpr::from_rel_node(op_0.left_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_0.index(), 4);
        let op_1 = BinOpExpr::from_rel_node(plan_filter_expr.children()[1].clone().into_rel_node())
            .unwrap();
        let col_1 =
            ColumnRefExpr::from_rel_node(op_1.left_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_1.index(), 7);
    }

    #[test]
    fn push_past_join_conjunction() {
        // Test pushing a complex filter past a join, where one clause can
        // be pushed to the left child, one to the right child, one gets incorporated
        // into the (now inner) join condition, and a constant one remains in the
        // original filter.
        let mut dummy_optimizer = new_dummy_optimizer();

        let scan1 = LogicalScan::new("customer".into());

        let scan2 = LogicalScan::new("orders".into());

        let join = LogicalJoin::new(
            scan1.into_plan_node(),
            scan2.into_plan_node(),
            LogOpExpr::new(
                LogOpType::And,
                ExprList::new(vec![BinOpExpr::new(
                    ColumnRefExpr::new(0).into_expr(),
                    ConstantExpr::int32(1).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr()]),
            )
            .into_expr(),
            super::JoinType::Inner,
        );

        let filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    // This one should be pushed to the left child
                    ColumnRefExpr::new(0).into_expr(),
                    ConstantExpr::int32(5).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // This one should be pushed to the right child
                    ColumnRefExpr::new(11).into_expr(),
                    ConstantExpr::int32(6).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // This one should be pushed to the join condition
                    ColumnRefExpr::new(2).into_expr(),
                    ColumnRefExpr::new(8).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // always true, should be removed by other rules
                    ConstantExpr::int32(2).into_expr(),
                    ConstantExpr::int32(7).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        );

        // Initialize groups
        dummy_optimizer
            .step_optimize_rel(join.clone().into_rel_node())
            .unwrap();

        dummy_optimizer
            .step_optimize_rel(join.clone().left().into_rel_node())
            .unwrap();

        dummy_optimizer
            .step_optimize_rel(join.clone().right().into_rel_node())
            .unwrap();

        let plan = apply_filter_pushdown(
            &dummy_optimizer,
            super::FilterPushdownRulePicks {
                child: Arc::unwrap_or_clone(join.into_rel_node()),
                cond: Arc::unwrap_or_clone(filter_expr.into_rel_node()),
            },
        );

        let plan = plan.first().unwrap();

        // Examine original filter + condition
        let top_level_filter = LogicalFilter::from_rel_node(plan.clone().into()).unwrap();
        let top_level_filter_cond =
            LogOpExpr::from_rel_node(top_level_filter.cond().into_rel_node()).unwrap();
        assert!(matches!(top_level_filter_cond.op_type(), LogOpType::And));
        assert!(matches!(top_level_filter_cond.children().len(), 1));
        let bin_op_0 =
            BinOpExpr::from_rel_node(top_level_filter_cond.children()[0].clone().into_rel_node())
                .unwrap();
        assert!(matches!(bin_op_0.op_type(), BinOpType::Eq));
        let col_0 =
            ConstantExpr::from_rel_node(bin_op_0.left_child().clone().into_rel_node()).unwrap();
        let col_1 =
            ConstantExpr::from_rel_node(bin_op_0.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_0.value().as_i32(), 2);
        assert_eq!(col_1.value().as_i32(), 7);

        // Examine join node + condition
        let join_node =
            LogicalJoin::from_rel_node(top_level_filter.child().clone().into_rel_node()).unwrap();
        let join_conds = LogOpExpr::from_rel_node(join_node.cond().into_rel_node()).unwrap();
        assert!(matches!(join_conds.op_type(), LogOpType::And));
        assert_eq!(join_conds.children().len(), 2);
        let bin_op_1 =
            BinOpExpr::from_rel_node(join_conds.children()[0].clone().into_rel_node()).unwrap();
        assert!(matches!(bin_op_1.op_type(), BinOpType::Eq));
        let col_2 =
            ColumnRefExpr::from_rel_node(bin_op_1.left_child().clone().into_rel_node()).unwrap();
        let col_3 =
            ColumnRefExpr::from_rel_node(bin_op_1.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_2.index(), 2);
        assert_eq!(col_3.index(), 8);

        // Examine left child filter + condition
        let filter_1 = LogicalFilter::from_rel_node(join_node.left().into_rel_node()).unwrap();
        let filter_1_cond = LogOpExpr::from_rel_node(filter_1.cond().into_rel_node()).unwrap();
        assert!(matches!(filter_1_cond.children().len(), 1));
        assert!(matches!(filter_1_cond.op_type(), LogOpType::And));
        let bin_op_3 =
            BinOpExpr::from_rel_node(filter_1_cond.children()[0].clone().into_rel_node()).unwrap();
        assert!(matches!(bin_op_3.op_type(), BinOpType::Eq));
        let col_6 =
            ColumnRefExpr::from_rel_node(bin_op_3.left_child().clone().into_rel_node()).unwrap();
        let col_7 =
            ConstantExpr::from_rel_node(bin_op_3.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_6.index(), 0);
        assert_eq!(col_7.value().as_i32(), 5);

        // Examine right child filter + condition
        let filter_2 = LogicalFilter::from_rel_node(join_node.right().into_rel_node()).unwrap();
        let filter_2_cond = LogOpExpr::from_rel_node(filter_2.cond().into_rel_node()).unwrap();
        assert!(matches!(filter_2_cond.op_type(), LogOpType::And));
        assert!(matches!(filter_2_cond.children().len(), 1));
        let bin_op_4 =
            BinOpExpr::from_rel_node(filter_2_cond.children()[0].clone().into_rel_node()).unwrap();
        assert!(matches!(bin_op_4.op_type(), BinOpType::Eq));
        let col_8 =
            ColumnRefExpr::from_rel_node(bin_op_4.left_child().clone().into_rel_node()).unwrap();
        let col_9 =
            ConstantExpr::from_rel_node(bin_op_4.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_8.index(), 3);
        assert_eq!(col_9.value().as_i32(), 6);
    }
}
