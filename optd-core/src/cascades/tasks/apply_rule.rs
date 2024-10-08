use std::{collections::HashMap, sync::Arc};

use itertools::Itertools;
use tracing::trace;

use crate::{
    cascades::{
        memo::RelMemoNodeRef,
        optimizer::{rule_matches_expr, ExprId, RuleId},
        tasks::{explore_expr::ExploreExprTask, optimize_inputs::OptimizeInputsTask},
        CascadesOptimizer, GroupId,
    },
    rel_node::{RelNode, RelNodeTyp},
    rules::{Rule, RuleMatcher},
};

use super::Task;

// Pick/match logic, to get pieces of info to pass to the rule apply function

fn match_node<T: RelNodeTyp>(
    typ: &T,
    children: &[RuleMatcher<T>],
    pick_to: Option<usize>,
    node: RelMemoNodeRef<T>,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    if let RuleMatcher::PickMany { .. } | RuleMatcher::IgnoreMany = children.last().unwrap() {
    } else {
        assert_eq!(
            children.len(),
            node.children.len(),
            "children size unmatched, please fix the rule: {}",
            node
        );
    }

    let mut should_end = false;
    let mut picks = vec![HashMap::new()];
    for (idx, child) in children.iter().enumerate() {
        assert!(!should_end, "many matcher should be at the end");
        match child {
            RuleMatcher::IgnoreOne => {}
            RuleMatcher::IgnoreMany => {
                should_end = true;
            }
            RuleMatcher::PickOne { pick_to, expand } => {
                let group_id = node.children[idx];
                let node = if *expand {
                    let mut exprs = optimizer.get_all_exprs_in_group(group_id);
                    assert_eq!(exprs.len(), 1, "can only expand expression");
                    let expr = exprs.remove(0);
                    let mut bindings = optimizer.get_all_expr_bindings(expr, None);
                    assert_eq!(bindings.len(), 1, "can only expand expression");
                    bindings.remove(0).as_ref().clone()
                } else {
                    RelNode::new_group(group_id)
                };
                for pick in &mut picks {
                    let res = pick.insert(*pick_to, node.clone());
                    assert!(res.is_none(), "dup pick");
                }
            }
            RuleMatcher::PickMany { pick_to } => {
                for pick in &mut picks {
                    let res = pick.insert(
                        *pick_to,
                        RelNode::new_list(
                            node.children[idx..]
                                .iter()
                                .map(|x| Arc::new(RelNode::new_group(*x)))
                                .collect_vec(),
                        ),
                    );
                    assert!(res.is_none(), "dup pick");
                }
                should_end = true;
            }
            _ => {
                let new_picks = match_and_pick_group(child, node.children[idx], optimizer);
                let mut merged_picks = vec![];
                for old_pick in &picks {
                    for new_picks in &new_picks {
                        let mut pick = old_pick.clone();
                        pick.extend(new_picks.iter().map(|(k, v)| (*k, v.clone())));
                        merged_picks.push(pick);
                    }
                }
                picks = merged_picks;
            }
        }
    }
    if let Some(pick_to) = pick_to {
        for pick in &mut picks {
            let res: Option<RelNode<T>> = pick.insert(
                pick_to,
                RelNode {
                    typ: typ.clone(),
                    children: node
                        .children
                        .iter()
                        .map(|x| RelNode::new_group(*x).into())
                        .collect_vec(),
                    data: node.data.clone(),
                },
            );
            assert!(res.is_none(), "dup pick");
        }
    }
    picks
}

fn match_and_pick_expr<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    expr_id: ExprId,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    let node = optimizer.get_expr_memoed(expr_id);
    match_and_pick(matcher, node, optimizer)
}

fn match_and_pick_group<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    group_id: GroupId,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    let mut matches = vec![];
    for expr_id in optimizer.get_all_exprs_in_group(group_id) {
        let node = optimizer.get_expr_memoed(expr_id);
        matches.extend(match_and_pick(matcher, node, optimizer));
    }
    matches
}

fn match_and_pick<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    node: RelMemoNodeRef<T>,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    match matcher {
        RuleMatcher::MatchAndPickNode {
            typ,
            children,
            pick_to,
        } => {
            if &node.typ != typ {
                return vec![];
            }
            match_node(typ, children, Some(*pick_to), node, optimizer)
        }
        RuleMatcher::MatchNode { typ, children } => {
            if &node.typ != typ {
                return vec![];
            }
            match_node(typ, children, None, node, optimizer)
        }
        RuleMatcher::MatchDiscriminant {
            typ_discriminant,
            children,
        } => {
            if std::mem::discriminant(&node.typ) != *typ_discriminant {
                return vec![];
            }
            match_node(&node.typ.clone(), children, None, node, optimizer)
        }
        RuleMatcher::MatchAndPickDiscriminant {
            typ_discriminant,
            children,
            pick_to,
        } => {
            if std::mem::discriminant(&node.typ) != *typ_discriminant {
                return vec![];
            }
            match_node(&node.typ.clone(), children, Some(*pick_to), node, optimizer)
        }
        _ => panic!("top node should be match node"),
    }
}

pub struct ApplyRuleTask<T: RelNodeTyp> {
    expr_id: ExprId,
    rule_id: RuleId,
    rule: Arc<dyn Rule<T, CascadesOptimizer<T>>>,
    // TODO: Promise here? Maybe it can be part of the Rule trait.
    cost_limit: Option<isize>,
}

impl<T: RelNodeTyp> ApplyRuleTask<T> {
    pub fn new(
        expr_id: ExprId,
        rule_id: RuleId,
        rule: Arc<dyn Rule<T, CascadesOptimizer<T>>>,
        cost_limit: Option<isize>,
    ) -> Self {
        Self {
            expr_id,
            rule_id,
            rule,
            cost_limit,
        }
    }
}

fn transform<T: RelNodeTyp>(
    optimizer: &CascadesOptimizer<T>,
    expr_id: ExprId,
    rule: &Arc<dyn Rule<T, CascadesOptimizer<T>>>,
) -> Vec<RelNode<T>> {
    // TODO(parallel): We may need memo lock for much of the matching process.
    // There should be a way to get the necessary info out in a short critical
    // section.
    let mut picked_data = match_and_pick_expr(rule.matcher(), expr_id, optimizer);
    assert!(
        picked_data.len() <= 1,
        "bad match count TODO(bowad) deal with this"
    );

    if picked_data.is_empty() {
        vec![]
    } else {
        rule.apply(optimizer, picked_data.remove(0))
    }
}

fn update_memo<T: RelNodeTyp>(
    optimizer: &CascadesOptimizer<T>,
    group_id: GroupId,
    new_exprs: Vec<Arc<RelNode<T>>>,
) -> Vec<ExprId> {
    let mut expr_ids = vec![];
    for new_expr in new_exprs {
        let (_, expr_id) = optimizer.add_expr_to_group(new_expr, group_id);
        expr_ids.push(expr_id);
    }
    expr_ids
}

/// TODO
///
/// Pseudocode:
/// function ApplyRule(expr, rule, promise, limit)
///     newExprs ← Transform(expr,rule)
///     UpdateMemo(newExprs)
///     Sort exprs by promise
///     for newExpr ∈ newExprs do
///         if Rule is a transformation rule then
///             tasks.Push(ExplExpr(newExpr, limit))
///         else
///             // Can fail if the cost limit becomes 0 or negative
///             limit ← UpdateCostLimit(newExpr, limit)
///             tasks.Push(OptInputs(newExpr, limit))
impl<T: RelNodeTyp> Task<T> for ApplyRuleTask<T> {
    fn execute(&self, optimizer: &CascadesOptimizer<T>) {
        trace!(event = "task_begin", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
        // TODO: Check transformation count and cancel invocation if we've hit
        // a limit
        debug_assert!(!optimizer.is_rule_applied(self.expr_id, self.rule_id));

        let expr = optimizer.get_expr_memoed(self.expr_id);
        let group_id = optimizer.get_group_id(self.expr_id);

        debug_assert!(rule_matches_expr(&self.rule, &expr));

        let new_exprs = transform(optimizer, self.expr_id, &self.rule);
        let new_exprs = new_exprs.into_iter().map(Arc::new).collect();
        let new_expr_ids = update_memo(optimizer, group_id, new_exprs);
        // TODO sort exprs by promise (??)
        for new_expr_id in new_expr_ids {
            let is_transformation_rule = !self.rule.is_impl_rule();
            if is_transformation_rule {
                // TODO: Increment transformation count
                optimizer.push_task(Box::new(ExploreExprTask::new(new_expr_id, self.cost_limit)));
            } else {
                let new_limit = None; // TODO: How do we update cost limit
                optimizer.push_task(Box::new(OptimizeInputsTask::new(new_expr_id, new_limit)));
            }
        }
        trace!(event = "task_finish", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
    }
}
