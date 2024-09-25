use tracing::trace;

use crate::{
    cascades::{
        optimizer::{rule_matches_expr, ExprId},
        CascadesOptimizer,
    },
    rel_node::RelNodeTyp,
};

use super::{apply_rule::ApplyRuleTask, explore_group::ExploreGroupTask, Task};

pub struct ExploreExprTask {
    expr_id: ExprId,
    cost_limit: Option<isize>,
}

impl ExploreExprTask {
    pub fn new(expr_id: ExprId, cost_limit: Option<isize>) -> Self {
        Self {
            expr_id,
            cost_limit,
        }
    }
}

/// ExploreExpr applies transformation rules to a single expression, to generate
/// more possible plans.
/// (Recall "transformation rules" are logical -> logical)
///
/// Pseudocode:
/// function ExplExpr(expr, limit)
///     moves ← ∅
///     for rule ∈ Transformation Rules do
///         // Can optionally apply guidance in if statement
///         if !expr.IsApplied(rule) and rule matches expr then
///             moves.Add(ApplyRule(expr, rule, promise, limit))
///     // Sort moves by promise
///     for m ∈ moves do
///         tasks.Push(m)
///     for childExpr in inputs of expr do
///         grp ← GetGroup(childExpr)
///         if !grp.Explored then
///             tasks.Push(ExplGrp(grp, limit))
impl<T: RelNodeTyp> Task<T> for ExploreExprTask {
    fn execute(&self, optimizer: &CascadesOptimizer<T>) {
        let expr = optimizer.get_expr_memoed(self.expr_id);
        let group_id = optimizer.get_group_id(self.expr_id);
        trace!(event = "task_begin", task = "explore_expr", group_id = %group_id, expr_id = %self.expr_id, expr = %expr);

        let mut moves = vec![];
        for (rule_id, rule) in optimizer.transformation_rules().iter() {
            let is_rule_applied = optimizer.is_rule_applied(self.expr_id, *rule_id);
            let rule_matches_expr = rule_matches_expr(rule, &expr);
            if !is_rule_applied && rule_matches_expr {
                moves.push(Box::new(ApplyRuleTask::new(
                    self.expr_id,
                    *rule_id,
                    rule.clone(),
                    self.cost_limit,
                )));
            }
        }
        // TODO: Sort moves by promise here
        for m in moves {
            // TODO: Add an optimized way to enqueue several tasks without
            // locking the tasks queue every time
            optimizer.push_task(m);
        }
        for child_group_id in expr.children.iter() {
            if !optimizer.is_group_explored(*child_group_id) {
                optimizer.push_task(Box::new(ExploreGroupTask::new(
                    *child_group_id,
                    self.cost_limit,
                )));
            }
        }
        trace!(event = "task_finish", task = "explore_expr", group_id = %group_id, expr_id = %self.expr_id, expr = %expr);
    }
}
