use anyhow::Result;
use tracing::trace;
use std::sync::Arc;

use crate::{
    cascades::{
        optimizer::{GroupId, SubGroupId, ExprId},
        memo::{SubGroupInfo, Winner},
        tasks::{optimize_expression::OptimizeExpressionTask, OptimizeInputsTask},
        CascadesOptimizer,
    },
    cost::Cost,
    rel_node::RelNodeTyp,
};

use super::Task;

pub struct OptimizeGroupTask {
    group_id: GroupId,
    return_from_optimize_group_without_required_physical_props: bool,
    required_physical_props: Arc<dyn RequiredPhysicalProps>
}

impl OptimizeGroupTask {
    pub fn new(group_id: GroupId, required_physical_props: Arc<dyn RequiredPhysicalProps>) -> Self {
        Self { group_id, return_from_optimize_group_without_required_physical_props:false, required_physical_props }
    }
    pub fn continue_from_optimize_group(&self) -> Self{
        Self { group_id: self.group_id, return_from_optimize_group_without_required_physical_props: true, required_physical_props: self.required_physical_props }
    }

    fn update_winner<T: RelNodeTyp>(
        &self,
        expr_id: ExprId,
        cost_so_far: &Cost,
        optimizer: &mut CascadesOptimizer<T>,
    ) {
        let sub_group_info = optimizer.get_sub_group_info(self.group_id, self.required_physical_props).unwrap();
    
        let mut update_cost = false;
        if let Some(ref winner) = sub_group_info.winner {
            if winner.impossible || &winner.cost > cost_so_far {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            optimizer.update_sub_group_info(
                self.group_id,
                Some(expr_id),
                SubGroupInfo {
                    winner: Some(Winner {
                        impossible: false,
                        expr_id: expr_id,
                        cost: cost_so_far.clone(),
                    }),
                    physical_props: self.required_physical_props.clone(),
                },
            );
        }
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeGroupTask {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        trace!(event = "task_begin", task = "optimize_group", group_id = %self.group_id);

        let group_info = optimizer.get_sub_group_info(self.group_id, self.required_physical_props);
        if group_info.is_some() && group_info.winner.is_some() {
            trace!(event = "task_finish", task = "optimize_group");
            return Ok(vec![]);
        }

        if self.return_from_optimize_group_without_required_physical_props {
            let group_info = optimizer.get_group_info(self.group_id);
            // the default sub group must have the winner
            assert!(group_info.winner.is_some() && !group_info.winner.unwrap().impossible, "after optimizeGroup without required physical props, the group must have a winner");
            let expr_id = group_info.winner.unwrap().expr_id;
            let expr = optimizer.get_expr_memoed(expr_id);
            //TODO: we need to get the children properties 
            let new_expr= self.required_physical_props.enforce(expr.typ.clone(), expr.data.clone());
            let expr_id = optimizer.add_group_expr(Some(self.group_id), new_expr);
            let cost_so_far = optimizer.get_expr_memoed(expr_id).cost; // new expr id cost
            optimizer.add_sub_group_expr(self.group_id, new_expr, self.required_physical_props);
            self.update_winner(expr_id, cost_so_far, optimizer); // update the winner for the sub group
            trace!(event = "task_finish", task = "optimize_group");
            return Ok(vec![]);
        }

        let mut tasks = vec![];
        if !self.required_physical_props.is_any(){
            // first push the return task
            tasks.push(self.continue_from_optimize_group() as Box<dyn Task<T>>);
            // try optimize group without required physical props and using enforcer to enforce them
            tasks.push(Box::new(OptimizeGroupTask::new(self.group_id, self.required_physical_props.Any())) as Box<dyn Task<T>>);
        }

        let exprs = optimizer.get_all_exprs_in_group(self.group_id);
        let exprs_cnt = exprs.len();
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                tasks.push(Box::new(OptimizeExpressionTask::new(expr, false, self.required_physical_props.Any())) as Box<dyn Task<T>>);
            }
        }
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if !typ.is_logical() {
                tasks.push(Box::new(OptimizeInputsTask::new(expr, true, self.required_physical_props)) as Box<dyn Task<T>>);
            }
        }
        trace!(event = "task_finish", task = "optimize_group", group_id = %self.group_id, exprs_cnt = exprs_cnt);
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!("optimize_group {}", self.group_id)
    }
}
