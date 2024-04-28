use anyhow::Result;
use tracing::trace;
use std::sync::Arc;

use crate::{
    cascades::{
        optimizer::{CascadesOptimizer, GroupId},
        tasks::OptimizeExpressionTask,
    },
    rel_node::RelNodeTyp,
    physical_prop::PhysicalPropsBuilder,
};

use super::Task;

pub struct ExploreGroupTask<T: RelNodeTyp, P: PhysicalPropsBuilder<T>> {
    group_id: GroupId,
    physical_props_builder: Arc<P>,
    required_physical_props: P::PhysicalProps,
}

impl<T:RelNodeTyp, P:PhysicalPropsBuilder<T>> ExploreGroupTask<T, P> {
    pub fn new(group_id: GroupId, physical_props_builder: Arc<P>, required_physical_props: P::PhysicalProps) -> Self {
        if !physical_props_builder.is_any(required_physical_props){
            unreachable!("ExploreGroupTask should not have any required physical properties")
        }
        Self { group_id, physical_props_builder, required_physical_props }
    }
}

impl<T: RelNodeTyp, P:PhysicalPropsBuilder<T>> Task<T,P> for ExploreGroupTask<T,P> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T, P>) -> Result<Vec<Box<dyn Task<T,P>>>> {
        trace!(event = "task_begin", task = "explore_group", group_id = %self.group_id);
        let mut tasks = vec![];
        if optimizer.is_group_explored(self.group_id) {
            trace!(target: "task_finish", task = "explore_group", result = "already explored, skipping", group_id = %self.group_id);
            return Ok(vec![]);
        }
        let exprs = optimizer.get_all_exprs_in_group(self.group_id);
        let exprs_cnt = exprs.len();
        for expr in exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                tasks.push(Box::new(OptimizeExpressionTask::new(expr, true, self.physical_props_builder.clone(), self.required_physical_props.clone())) as Box<dyn Task<T,P>>);
            }
        }
        optimizer.mark_group_explored(self.group_id);
        trace!(
            event = "task_finish",
            task = "explore_group",
            result = "expand group",
            exprs_cnt = exprs_cnt
        );
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!("explore_group {}", self.group_id)
    }
}
