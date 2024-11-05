use optimize_group::OptimizeGroupTask;

use crate::nodes::NodeType;

use super::{CascadesOptimizer, GroupId, Memo};

mod apply_rule;
mod explore_expr;
mod explore_group;
mod optimize_expr;
mod optimize_group;
mod optimize_inputs;

pub trait Task<T: NodeType, M: Memo<T>>: 'static + Send + Sync {
    fn execute(&self, optimizer: &CascadesOptimizer<T, M>);
}

pub fn get_initial_task<T: NodeType, M: Memo<T>>(
    initial_task_id: usize,
    root_group_id: GroupId,
) -> Box<dyn Task<T, M>> {
    Box::new(OptimizeGroupTask::new(
        None,
        initial_task_id,
        root_group_id,
        None,
    ))
}
