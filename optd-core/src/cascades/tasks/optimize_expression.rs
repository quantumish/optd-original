use anyhow::Result;
use tracing::trace;
use std::sync::Arc;


use crate::{
    cascades::{
        optimizer::{CascadesOptimizer, ExprId},
        tasks::{ApplyRuleTask, ExploreGroupTask},
    },
    rel_node::{RelNodeTyp, Value},
    rules::RuleMatcher,
    physical_prop::PhysicalProps
};

use super::Task;

pub struct OptimizeExpressionTask<T: RelNodeTyp> {
    expr_id: ExprId,
    exploring: bool,
    required_physical_props: Arc<dyn PhysicalProps<T>>,
}

impl<T:RelNodeTyp> OptimizeExpressionTask<T> {
    pub fn new(expr_id: ExprId, exploring: bool, required_physical_props: Arc<dyn PhysicalProps<T>>) -> Self {
        Self { expr_id, exploring, required_physical_props }
    }
}

fn top_matches<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    match_typ: T,
    _data: Option<Value>,
) -> bool {
    match matcher {
        RuleMatcher::MatchAndPickNode { typ, .. } => typ == &match_typ,
        RuleMatcher::MatchNode { typ, .. } => typ == &match_typ,
        _ => panic!("IR should have root node of match"),
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeExpressionTask<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        let expr = optimizer.get_expr_memoed(self.expr_id);
        trace!(event = "task_begin", task = "optimize_expr", expr_id = %self.expr_id, expr = %expr);
        let mut tasks = vec![];
        for (rule_id, rule_wrapper) in optimizer.rules().iter().enumerate() {
            let rule = rule_wrapper.rule();
            if optimizer.is_rule_fired(self.expr_id, rule_id) {
                continue;
            }
            // Skip impl rules when exploring
            if self.exploring && rule.is_impl_rule() {
                continue;
            }
            // Skip transformation rules when budget is used
            if optimizer.ctx.budget_used && !rule.is_impl_rule() {
                continue;
            }
            if top_matches(rule.matcher(), expr.typ.clone(), expr.data.clone()) {
                tasks.push(
                    Box::new(ApplyRuleTask::new(rule_id, self.expr_id, self.exploring, self.required_physical_props.clone()))
                        as Box<dyn Task<T>>,
                );
                for &input_group_id in &expr.children {
                    // Explore the whole group instead of the specigic SubGroup the expr children points to
                    // As explore task is for logical transformations
                    tasks.push(Box::new(ExploreGroupTask::new(input_group_id.0, self.required_physical_props.clone())) as Box<dyn Task<T>>);
                }
            }
        }
        trace!(event = "task_end", task = "optimize_expr", expr_id = %self.expr_id);
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!("optimize_expr {}", self.expr_id)
    }
}
