use anyhow::Result;
use tracing::trace;
use std::sync::Arc;

use crate::{
    cascades::{
        optimizer::GroupId,
        tasks::{optimize_expression::OptimizeExpressionTask, OptimizeInputsTask},
        CascadesOptimizer,
    },
    physical_prop::PhysicalPropsBuilder,
    rel_node::RelNodeTyp,
};

use super::Task;

/// OptimizeGroupTask calls
///     1. OptimizeExpressionTask for all logical expressions in the group
///     2. OptimizeInputsTask for all physical expressions in the group
/// For required physical properties, it passes them to OptimizeInputTask and OptimizeExpressionTask
pub struct OptimizeGroupTask<T: RelNodeTyp, P: PhysicalPropsBuilder<T>> {
    group_id: GroupId,
    physical_props_builder: Arc<P>,
    required_physical_props: P::PhysicalProps,
}

impl<T:RelNodeTyp, P: PhysicalPropsBuilder<T>> OptimizeGroupTask<T, P> {
    pub fn new(group_id: GroupId, physical_props_builder: Arc<P>, required_physical_props: P::PhysicalProps) -> Self {
        Self { group_id, physical_props_builder, required_physical_props }
    }
}

impl<T: RelNodeTyp, P:PhysicalPropsBuilder<T>> Task<T,P> for OptimizeGroupTask<T, P> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T, P>) -> Result<Vec<Box<dyn Task<T,P>>>> {
        trace!(event = "task_begin", task = "optimize_group", group_id = %self.group_id);

        let group_info = optimizer.get_sub_group_info_by_props(self.group_id, self.required_physical_props);
        if group_info.is_some() && group_info.unwrap().winner.is_some() {
            trace!(event = "task_finish", task = "optimize_group");
            return Ok(vec![]);
        }

        let mut tasks = vec![];
        let exprs = optimizer.get_all_exprs_in_group(self.group_id);
        let exprs_cnt = exprs.len();
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                tasks.push(Box::new(OptimizeExpressionTask::new(expr, false, self.physical_props_builder.clone(), self.required_physical_props.clone())) as Box<dyn Task<T,P>>);
            }
        }
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if !typ.is_logical() {
                tasks.push(Box::new(OptimizeInputsTask::new(expr, true, self.physical_props_builder.clone(), self.required_physical_props.clone())) as Box<dyn Task<T,P>>);
            }
        }
        trace!(event = "task_finish", task = "optimize_group", group_id = %self.group_id, exprs_cnt = exprs_cnt);
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!("optimize_group {}", self.group_id)
    }
}
