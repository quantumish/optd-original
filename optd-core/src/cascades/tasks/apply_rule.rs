use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use itertools::Itertools;
use tracing::trace;

use crate::{
    cascades::{
        memo::RelMemoNodeRef,
        optimizer::{CascadesOptimizer, ExprId, RuleId},
        tasks::{OptimizeExpressionTask, OptimizeInputsTask},
        GroupId,
    },
    rel_node::{RelNode, RelNodeTyp},
    rules::RuleMatcher,
};

use super::Task;

/// Applys a rule to a multi-expression.
pub struct ApplyRuleTask {
    rule_id: RuleId,
    expr_id: ExprId,
    /// When this flag is on, only fire transformation rules for the expression (no implementation rule).
    exploring: bool,
}

impl ApplyRuleTask {
    pub fn new(rule_id: RuleId, expr_id: ExprId, exploring: bool) -> Self {
        Self {
            rule_id,
            expr_id,
            exploring,
        }
    }
}

fn match_node<T: RelNodeTyp>(
    typ: &T,
    children: &[RuleMatcher<T>],
    pick_to: Option<usize>, // integer id for an user-requested expression (unique within a matcher)
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

fn match_and_pick_group<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    group_id: GroupId,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    let mut matches = vec![];
    for expr_id in optimizer.get_all_exprs_in_group(group_id) {
        // get corresponding expr memoed.
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
            // check node type
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
        _ => panic!("top node should be match node"),
    }
}

impl<T: RelNodeTyp> Task<T> for ApplyRuleTask {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        // rule has been fired against mexpr
        if optimizer.is_rule_fired(self.expr_id, self.rule_id) {
            return Ok(vec![]);
        }
        
        if optimizer.is_rule_disabled(self.rule_id) {
            optimizer.mark_rule_fired(self.expr_id, self.rule_id);
            return Ok(vec![]);
        }

        let rule = optimizer.rules()[self.rule_id].clone();
        trace!(event = "task_begin", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, rule = %rule.name());
        // get the group id of the group whose root has expr_id
        let group_id = optimizer.get_group_id(self.expr_id);
        let mut tasks = vec![];
        let binding_exprs = match_and_pick_group(rule.matcher(), group_id, optimizer);
        for expr in binding_exprs {
            let applied = rule.apply(optimizer, expr);
            for expr in applied {
                let RelNode { typ, .. } = &expr;
                if typ.extract_group().is_some() {
                    unreachable!();
                }
                let expr_typ = typ.clone();
                let (_, expr_id) = optimizer.add_group_expr(expr.into(), Some(group_id));
                trace!(event = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, new_expr_id = %expr_id);
                if expr_typ.is_logical() {
                    tasks.push(
                        Box::new(OptimizeExpressionTask::new(expr_id, self.exploring))
                            as Box<dyn Task<T>>,
                    );
                } else {
                    tasks
                        .push(Box::new(OptimizeInputsTask::new(expr_id, true)) as Box<dyn Task<T>>);
                }
            }
        }
        optimizer.mark_rule_fired(self.expr_id, self.rule_id);

        trace!(event = "task_end", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!(
            "apply_rule {{ rule_id: {}, expr_id: {}, exploring: {} }}",
            self.rule_id, self.expr_id, self.exploring
        )
    }
}
