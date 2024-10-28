use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{nodes::PlanNode, optimizer::Optimizer};

use crate::plan_nodes::{
    DfNodeType, DfReprPlanNode, DfReprPlanNode, Expr, ListPred, LogicalAgg, LogicalSort,
    SortOrderPred, SortOrderType,
};

use super::macros::define_rule;

define_rule!(
    EliminateDuplicatedSortExprRule,
    apply_eliminate_duplicated_sort_expr,
    (Sort, child, [exprs])
);

/// Removes duplicate sort expressions
/// For exmaple:
///     select *
///     from t1
///     order by id desc, id, name, id asc
/// becomes
///     select *
///     from t1
///     order by id desc, name
fn apply_eliminate_duplicated_sort_expr(
    _optimizer: &impl Optimizer<DfNodeType>,
    EliminateDuplicatedSortExprRulePicks { child, exprs }: EliminateDuplicatedSortExprRulePicks,
) -> Vec<PlanNode<DfNodeType>> {
    let sort_keys: Vec<Expr> = exprs
        .children
        .iter()
        .map(|x| Expr::from_rel_node(x.clone()).unwrap())
        .collect_vec();

    let normalized_sort_keys: Vec<Arc<PlanNode<DfNodeType>>> = exprs
        .children
        .iter()
        .map(|x| match x.typ {
            DfNodeType::SortOrder(_) => SortOrderPred::new(
                SortOrderType::Asc,
                SortOrderPred::from_rel_node(x.clone()).unwrap().child(),
            )
            .into_rel_node(),
            _ => x.clone(),
        })
        .collect_vec();

    let mut dedup_expr: Vec<Expr> = Vec::new();
    let mut dedup_set: HashSet<Arc<PlanNode<DfNodeType>>> = HashSet::new();

    sort_keys
        .iter()
        .zip(normalized_sort_keys.iter())
        .for_each(|(expr, normalized_expr)| {
            if !dedup_set.contains(normalized_expr) {
                dedup_expr.push(expr.clone());
                dedup_set.insert(normalized_expr.to_owned());
            }
        });

    if dedup_expr.len() != sort_keys.len() {
        let node = LogicalSort::new(
            DfReprPlanNode::from_group(child.into()),
            ListPred::new(dedup_expr),
        );
        return vec![node.into_rel_node().as_ref().clone()];
    }
    vec![]
}

define_rule!(
    EliminateDuplicatedAggExprRule,
    apply_eliminate_duplicated_agg_expr,
    (Agg, child, exprs, [groups])
);

/// Removes duplicate group by expressions
/// For exmaple:
///     select *
///     from t1
///     group by id, name, id, id
/// becomes
///     select *
///     from t1
///     group by id, name
fn apply_eliminate_duplicated_agg_expr(
    _optimizer: &impl Optimizer<DfNodeType>,
    EliminateDuplicatedAggExprRulePicks {
        child,
        exprs,
        groups,
    }: EliminateDuplicatedAggExprRulePicks,
) -> Vec<PlanNode<DfNodeType>> {
    let mut dedup_expr: Vec<Expr> = Vec::new();
    let mut dedup_set: HashSet<Arc<PlanNode<DfNodeType>>> = HashSet::new();
    groups.children.iter().for_each(|expr| {
        if !dedup_set.contains(expr) {
            dedup_expr.push(Expr::from_rel_node(expr.clone()).unwrap());
            dedup_set.insert(expr.clone());
        }
    });

    if dedup_expr.len() != groups.children.len() {
        let node = LogicalAgg::new(
            DfReprPlanNode::from_group(child.into()),
            ListPred::from_group(exprs.into()),
            ListPred::new(dedup_expr),
        );
        return vec![node.into_rel_node().as_ref().clone()];
    }
    vec![]
}
