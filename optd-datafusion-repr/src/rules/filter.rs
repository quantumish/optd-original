use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::macros::define_rule;
use crate::plan_nodes::{
    ConstantPred, ConstantType, DfNodeType, DfReprPlanNode, DfReprPlanNode, Expr, JoinType,
    ListPred, LogOpPred, LogOpType, LogicalEmptyRelation, LogicalJoin,
};
use crate::properties::schema::SchemaPropertyBuilder;
use crate::ArcDfPlanNode;
use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{nodes::PlanNode, optimizer::Optimizer};

define_rule!(
    SimplifyFilterRule,
    apply_simplify_filter,
    (Filter, child, [cond])
);

// simplify_log_expr simplifies the Filters operator in several possible
//  ways:
//    - Replaces the Or operator with True if any operand is True
//    - Replaces the And operator with False if any operand is False
//    - Removes Duplicates
pub(crate) fn simplify_log_expr(log_expr: ArcDfPlanNode, changed: &mut bool) -> ArcDfPlanNode {
    let log_expr = LogOpPred::from_rel_node(log_expr).unwrap();
    let op = log_expr.op_type();
    // we need a new children vec to output deterministic order
    let mut new_children_set = HashSet::new();
    let mut new_children = Vec::new();
    let children_size = log_expr.children().len();
    for child in log_expr.children() {
        let mut new_child = child;
        if let DfNodeType::LogOp(_) = new_child.typ() {
            let new_expr = simplify_log_expr(new_child.into_rel_node().clone(), changed);
            new_child = Expr::from_rel_node(new_expr).unwrap();
        }
        if let DfNodeType::Constant(ConstantType::Bool) = new_child.typ() {
            let data = new_child.into_rel_node().data.clone().unwrap();
            *changed = true;
            // TrueExpr
            if data.as_bool() {
                if op == LogOpType::And {
                    // skip True in And
                    continue;
                }
                if op == LogOpType::Or {
                    // replace whole exprList with True
                    return ConstantPred::bool(true).into_rel_node().clone();
                }
                unreachable!("no other type in logOp");
            }
            // FalseExpr
            if op == LogOpType::And {
                // replace whole exprList with False
                return ConstantPred::bool(false).into_rel_node().clone();
            }
            if op == LogOpType::Or {
                // skip False in Or
                continue;
            }
            unreachable!("no other type in logOp");
        } else if !new_children_set.contains(&new_child) {
            new_children_set.insert(new_child.clone());
            new_children.push(new_child);
        }
    }
    if new_children.is_empty() {
        if op == LogOpType::And {
            return ConstantPred::bool(true).into_rel_node().clone();
        }
        if op == LogOpType::Or {
            return ConstantPred::bool(false).into_rel_node().clone();
        }
        unreachable!("no other type in logOp");
    }
    if new_children.len() == 1 {
        *changed = true;
        return new_children
            .into_iter()
            .next()
            .unwrap()
            .into_rel_node()
            .clone();
    }
    if children_size != new_children.len() {
        *changed = true;
    }
    LogOpPred::new(op, ListPred::new(new_children))
        .into_rel_node()
        .clone()
}

// SimplifySelectFilters simplifies the Filters operator in several possible
//  ways:
//    - Replaces the Or operator with True if any operand is True
//    - Replaces the And operator with False if any operand is False
//    - Removes Duplicates
fn apply_simplify_filter(
    _optimizer: &impl Optimizer<DfNodeType>,
    SimplifyFilterRulePicks { child, cond }: SimplifyFilterRulePicks,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    match cond.typ {
        DfNodeType::LogOp(_) => {
            let mut changed = false;
            let new_log_expr = simplify_log_expr(Arc::new(cond), &mut changed);
            if changed {
                let filter_node = PlanNode {
                    typ: DfNodeType::Filter,
                    children: vec![child.into(), new_log_expr],
                    data: None,
                };
                return vec![filter_node];
            }
            vec![]
        }
        _ => {
            vec![]
        }
    }
}

// Same as SimplifyFilterRule, but for innerJoin conditions
define_rule!(
    SimplifyJoinCondRule,
    apply_simplify_join_cond,
    (Join(JoinType::Inner), left, right, [cond])
);

fn apply_simplify_join_cond(
    _optimizer: &impl Optimizer<DfNodeType>,
    SimplifyJoinCondRulePicks { left, right, cond }: SimplifyJoinCondRulePicks,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    match cond.typ {
        DfNodeType::LogOp(_) => {
            let mut changed = false;
            let new_log_expr = simplify_log_expr(Arc::new(cond), &mut changed);
            if changed {
                let join_node = LogicalJoin::new(
                    DfReprPlanNode::from_group(left.into()),
                    DfReprPlanNode::from_group(right.into()),
                    Expr::from_rel_node(new_log_expr).unwrap(),
                    JoinType::Inner,
                );
                return vec![join_node.into_rel_node().as_ref().clone()];
            }
            vec![]
        }
        _ => {
            vec![]
        }
    }
}

define_rule!(
    EliminateFilterRule,
    apply_eliminate_filter,
    (Filter, child, [cond])
);

/// Transformations:
///     - Filter node w/ false pred -> EmptyRelation
///     - Filter node w/ true pred  -> Eliminate from the tree
fn apply_eliminate_filter(
    optimizer: &impl Optimizer<DfNodeType>,
    EliminateFilterRulePicks { child, cond }: EliminateFilterRulePicks,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    if let DfNodeType::Constant(ConstantType::Bool) = cond.typ {
        if let Some(data) = cond.data {
            if data.as_bool() {
                // If the condition is true, eliminate the filter node, as it
                // will yield everything from below it.
                return vec![child];
            } else {
                // If the condition is false, replace this node with the empty relation,
                // since it will never yield tuples.
                let schema =
                    optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(child.clone()), 0);
                let node = LogicalEmptyRelation::new(false, schema);
                return vec![node.into_rel_node().as_ref().clone()];
            }
        }
    }
    vec![]
}
