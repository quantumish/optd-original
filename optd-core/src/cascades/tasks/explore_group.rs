use tracing::trace;

use crate::{
    cascades::{CascadesOptimizer, GroupId},
    rel_node::RelNodeTyp,
};

use super::{explore_expr::ExploreExprTask, Task};

pub struct ExploreGroupTask {
    group_id: GroupId,
    cost_limit: Option<isize>,
}

impl ExploreGroupTask {
    pub fn new(group_id: GroupId, cost_limit: Option<isize>) -> Self {
        Self {
            group_id,
            cost_limit,
        }
    }
}

/// ExploreGroup will apply transformation rules to generate more logical
/// expressions (or "explore" more logical expressions). It does this by
/// invoking the ExploreExpr task on every expression in the group.
/// (Recall "transformation rules" are logical -> logical)
///
/// Pseudocode:
/// function ExplGrp(grp, limit)
///     grp.Explored ← true
///     for expr ∈ grp.Expressions do
///         tasks.Push(ExplExpr(expr, limit))
impl<T: RelNodeTyp> Task<T> for ExploreGroupTask {
    fn execute(&self, optimizer: &CascadesOptimizer<T>) {
        trace!(event = "task_begin", task = "explore_group", group_id = %self.group_id);
        optimizer.mark_group_explored(self.group_id);
        for expr in optimizer.get_all_exprs_in_group(self.group_id) {
            optimizer.enqueue_task(Box::new(ExploreExprTask::new(expr, self.cost_limit)));
        }
        trace!(event = "task_finish", task = "explore_group", group_id = %self.group_id);
    }
}
