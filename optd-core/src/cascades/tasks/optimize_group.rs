use tracing::trace;

use crate::{
    cascades::{CascadesOptimizer, GroupId},
    rel_node::RelNodeTyp,
};

use super::{explore_group::ExploreGroupTask, optimize_expr::OptimizeExprTask, Task};

pub struct OptimizeGroupTask {
    group_id: GroupId,
    cost_limit: Option<isize>,
}

impl OptimizeGroupTask {
    pub fn new(group_id: GroupId, cost_limit: Option<isize>) -> Self {
        Self {
            group_id,
            cost_limit,
        }
    }
}

/// OptimizeGroup will find the best physical plan for the group.
/// It does this by applying implementation rules through the OptimizeExpr task
/// (Recall "implementation rules" are logical -> physical)
///
/// Before it tries to generate different physical plans, it will invoke
/// explore tasks to generate more logical expressions through transformation
/// rules.
///
/// Pseudocode:
/// function OptGrp(expr, limit)
///     grp ← GetGroup(expr)
///     if !grp.Explored then
///         tasks.Push(OptGrp(grp, limit))
///         tasks.Push(ExplGrp(grp, limit))
///     else
///         for expr ∈ grp.Expressions do
///         tasks.Push(OptExpr(expr, limit))
impl<T: RelNodeTyp> Task<T> for OptimizeGroupTask {
    fn execute(&self, optimizer: &CascadesOptimizer<T>) {
        trace!(event = "task_begin", task = "optimize_group", group_id = %self.group_id);
        let group_explored = optimizer.is_group_explored(self.group_id);

        // Apply transformation rules *before* trying to apply our
        // implementation rules. (Task dependency enforced via stack push order)
        if !group_explored {
            // TODO(parallel): Task dependency here
            optimizer.push_task(Box::new(OptimizeGroupTask::new(
                self.group_id,
                self.cost_limit,
            )));
            optimizer.push_task(Box::new(ExploreGroupTask::new(
                self.group_id,
                self.cost_limit,
            )));
        } else {
            // Optimize every expression in the group
            // (apply implementation rules)
            for expr in optimizer.get_all_exprs_in_group(self.group_id) {
                optimizer.push_task(Box::new(OptimizeExprTask::new(expr, self.cost_limit)));
            }
        }
        trace!(event = "task_finish", task = "optimize_group", group_id = %self.group_id);
    }
}
