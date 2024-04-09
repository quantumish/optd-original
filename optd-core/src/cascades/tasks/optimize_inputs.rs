use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{
        memo::{SubGroupInfo, Winner},
        optimizer::ExprId,
        tasks::OptimizeGroupTask,
        CascadesOptimizer, GroupId, RelNodeContext,
    },
    cost::Cost,
    rel_node::RelNodeTyp,
};

use super::Task;

#[derive(Debug, Clone)]
struct ContinueTask {
    next_group_idx: usize,
    input_cost: Vec<Cost>,
    return_from_optimize_group: bool,
}

pub struct OptimizeInputsTask {
    expr_id: ExprId,
    continue_from: Option<ContinueTask>,
    pruning: bool,
    required_physical_props: Arc<dyn RequiredPhysicalProps>,
    required_children_props: Option<Vec<Arc<dyn RequiredPhysicalProps>>>
}

impl OptimizeInputsTask {
    pub fn new(expr_id: ExprId, pruning: bool, required_physical_props: Arc<dyn RequiredPhysicalProps>) -> Self {
        Self {
            expr_id,
            continue_from: None,
            pruning,
            required_physical_props,
            required_children_props: None
        }
    }

    fn continue_from(&self, cont: ContinueTask, pruning: bool) -> Self {
        Self {
            expr_id: self.expr_id,
            continue_from: Some(cont),
            pruning,
            required_physical_props: self.required_physical_props.clone(),
            required_children_props: self.required_children_props.clone()
        }
    }

    /// first invoke of this task, compute the cost of children
    fn first_invoke<T: RelNodeTyp>(
        &self,
        children: &[GroupId],
        optimizer: &mut CascadesOptimizer<T>,
    ) -> Vec<Cost> {
        let zero_cost = optimizer.cost().zero();
        let mut input_cost = Vec::with_capacity(children.len());
        for (&child, &prop) in children.iter().zip(self.required_children_props.unwrap().iter()) {
            let group = optimizer.get_sub_group_info(child, prop);
            if group.is_some() && group.winner.is_some() {
                let winner = group.winner.unwrap();
                if !winner.impossible {
                    // the full winner case
                    input_cost.push(winner.cost.clone());
                    continue;
                }
            }
            input_cost.push(zero_cost.clone());
        }
        input_cost
    }

    fn should_terminate(&self, cost_so_far: f64, upper_bound: Option<f64>) -> bool {
        if !self.pruning {
            return false;
        }
        if upper_bound.is_none() {
            return false;
        }
        let upper_bound = upper_bound.unwrap();
        if cost_so_far >= upper_bound {
            trace!(
                event = "optimize_inputs_pruning",
                task = "optimize_inputs_pruning",
                cost_so_far = cost_so_far,
                upper_bound = upper_bound
            );
            return true;
        }
        false
    }

    fn update_winner<T: RelNodeTyp>(
        &self,
        cost_so_far: &Cost,
        optimizer: &mut CascadesOptimizer<T>,
    ) {
        let expr_id = optimizer.update_expr_children_sub_group_id(self.expr_id, self.required_children_props.clone());
        let group_id = optimizer.get_group_id(expr_id);
        let sub_group_info = optimizer.get_sub_group_info(group_id, self.required_physical_props);
        let mut update_cost = false;
        if sub_group_info.is_some() && sub_group_info.winner.is_some() {
            let winner = sub_group_info.winner.unwrap();
            if winner.impossible || &winner.cost > cost_so_far {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            optimizer.update_sub_group_info(
                group_id,
                Some(expr_id),
                SubGroupInfo {
                    winner: Some(Winner {
                        impossible: false,
                        expr_id,
                        cost: cost_so_far.clone(),
                    }),
                    physical_props: self.required_physical_props.clone(),
                },
            );
        }
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeInputsTask {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        if optimizer.tasks.iter().any(|t| {
            if let Some(task) = t.as_any().downcast_ref::<Self>() {
                // skip optimize_inputs to avoid dead-loop: consider join commute being fired twice that produces
                // two projections, therefore having groups like projection1 -> projection2 -> join = projection1.
                task.expr_id == self.expr_id && task.required_physical_props == self.required_physical_props
            } else {
                false
            }
        }) {
            trace!(event = "task_skip", task = "optimize_inputs", expr_id = %self.expr_id);
            return Ok(vec![]);
        }
        trace!(event = "task_begin", task = "optimize_inputs", expr_id = %self.expr_id, continue_from = ?self.continue_from);
        let expr = optimizer.get_expr_memoed(self.expr_id);
        let group_id = optimizer.get_group_id(self.expr_id);
        let children_group_ids = &expr.children;
        let cost = optimizer.cost();

        if let Some(ContinueTask {
            next_group_idx,
            mut input_cost,
            return_from_optimize_group,
        }) = self.continue_from.clone()
        {
            let context = RelNodeContext {
                expr_id: self.expr_id,
                group_id,
                children_group_ids: children_group_ids.clone(),
            };
            if self.should_terminate(
                cost.sum(
                    &cost.compute_cost(
                        &expr.typ,
                        &expr.data,
                        &input_cost,
                        Some(context.clone()),
                        Some(optimizer),
                    ),
                    &input_cost,
                )
                .0[0],
                optimizer.ctx.upper_bound,
            ) {
                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                return Ok(vec![]);
            }
            if next_group_idx < children_group_ids.len() {
                let group_id = children_group_ids[next_group_idx];
                let group_idx = next_group_idx;
                let required_child_physical_props = self.required_children_props[group_idx].clone();
                let sub_group_info = optimizer.get_sub_group_info(group_id, required_child_physical_props);
                let mut has_full_winner = false;
                if sub_group_info.is_some() && sub_group_info.winner.is_some() {
                    let winner = sub_group_info.winner.unwrap();
                    if !winner.impossible {
                        input_cost[group_idx] = winner.cost.clone();
                        has_full_winner = true;
                        if self.should_terminate(
                            cost.sum(
                                &cost.compute_cost(
                                    &expr.typ,
                                    &expr.data,
                                    &input_cost,
                                    Some(context.clone()),
                                    Some(optimizer),
                                ),
                                &input_cost,
                            )
                            .0[0],
                            optimizer.ctx.upper_bound,
                        ) {
                            trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                            return Ok(vec![]);
                        }
                    }
                }
                if !has_full_winner {
                    if !return_from_optimize_group {
                        trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, group_idx = %group_idx);
                        return Ok(vec![
                            Box::new(self.continue_from(
                                ContinueTask {
                                    next_group_idx,
                                    input_cost,
                                    return_from_optimize_group: true,
                                },
                                self.pruning,
                            )) as Box<dyn Task<T>>,
                            Box::new(OptimizeGroupTask::new(group_id, required_child_physical_props)) as Box<dyn Task<T>>,
                        ]);
                    } else {
                        if sub_group_info.is_some() && sub_group_info.winner.is_some() {
                            let winner = sub_group_info.winner.unwrap();
                            if winner.impossible {
                                optimizer.update_sub_group_info(
                                    group_id,
                                    None, // No need to add this expr to the subgroup, as this expr cannot provide required physical props
                                    SubGroupInfo {
                                        winner: Some(Winner {
                                            impossible: true,
                                            ..Default::default()
                                        }),
                                        physical_props: required_child_physical_props,
                                    },
                                );
                                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                                return Ok(vec![]);
                            }
                        }
                        optimizer.update_sub_group_info(
                            group_id,
                            None,
                            SubGroupInfo {
                                winner: Some(Winner {
                                    impossible: true,
                                    ..Default::default()
                                }),
                                physical_props: required_child_physical_props,
                            },
                        );
                        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                        return Ok(vec![]);
                    }
                }
                trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, group_idx = %group_idx);
                Ok(vec![Box::new(self.continue_from(
                    ContinueTask {
                        next_group_idx: group_idx + 1,
                        input_cost,
                        return_from_optimize_group: false,
                    },
                    self.pruning,
                )) as Box<dyn Task<T>>])
            } else {
                self.update_winner(
                    &cost.sum(
                        &cost.compute_cost(
                            &expr.typ,
                            &expr.data,
                            &input_cost,
                            Some(context.clone()),
                            Some(optimizer),
                        ),
                        &input_cost,
                    ),
                    optimizer,
                );
                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                Ok(vec![])
            }
        } else {
            // three situations we can provide the required physical properties:
            // 1. if current expr can provide the required physical properties like sort merge join can provide ordering
            // 2. if current expr can pass the required physical properties to its children, like select, project can pass ordering to children
            // 3. no required_children_props constraints, like any ordering
            // One situation we can't provide the required physical properties:
            // 1. current expr cannot provide nor pass the required physical properties to its children, like hash join 
            if !self.required_physical_props.can_provide(){
                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                return Ok(vec![]);
            }
            // we leave the passing rules of required physical properties completely to the user
            // 1. if current expr can provide the required physical props, like sort merge join, the required physical props for children should be Any
            // 2. if current expr cannot provide, the required physical props should be assigned to children
            self.required_children_props = Some(self.required_physical_props.build_children_properties(expr.typ.clone(), expr.data.clone(), children_group_ids.len()));

            let input_cost = self.first_invoke(children_group_ids, optimizer);
            trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id);
            Ok(vec![Box::new(self.continue_from(
                ContinueTask {
                    next_group_idx: 0,
                    input_cost,
                    return_from_optimize_group: false,
                },
                self.pruning,
            )) as Box<dyn Task<T>>])
        }
    }

    fn describe(&self) -> String {
        format!("optimize_inputs {}", self.expr_id)
    }
}
