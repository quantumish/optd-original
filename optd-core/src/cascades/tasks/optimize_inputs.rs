use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{
        memo::{GroupInfo, Winner, WinnerInfo},
        optimizer::ExprId,
        tasks::OptimizeGroupTask,
        CascadesOptimizer, RelNodeContext,
    },
    cost::{Cost, Statistics},
    rel_node::RelNodeTyp,
};

use super::Task;

#[derive(Debug, Clone)]
struct ContinueTask {
    next_group_idx: usize,
    return_from_optimize_group: bool,
}

pub struct OptimizeInputsTask {
    expr_id: ExprId,
    continue_from: Option<ContinueTask>,
    pruning: bool,
}

impl OptimizeInputsTask {
    pub fn new(expr_id: ExprId, pruning: bool) -> Self {
        Self {
            expr_id,
            continue_from: None,
            pruning,
        }
    }

    fn continue_from(&self, cont: ContinueTask, pruning: bool) -> Self {
        Self {
            expr_id: self.expr_id,
            continue_from: Some(cont),
            pruning,
        }
    }

    fn update_winner_impossible<T: RelNodeTyp>(&self, optimizer: &mut CascadesOptimizer<T>) {
        let group_id = optimizer.get_group_id(self.expr_id);
        if let Winner::Unknown = optimizer.get_group_info(group_id).winner {
            optimizer.update_group_info(
                group_id,
                GroupInfo {
                    winner: Winner::Impossible,
                },
            );
        }
    }

    fn update_winner<T: RelNodeTyp>(
        &self,
        input_statistics: Vec<Option<&Statistics>>,
        operation_cost: Cost,
        total_cost: Cost,
        optimizer: &mut CascadesOptimizer<T>,
    ) {
        let group_id = optimizer.get_group_id(self.expr_id);
        let group_info = optimizer.get_group_info(group_id);
        let cost = optimizer.cost();
        let operation_weighted_cost = cost.weighted_cost(&operation_cost);
        let total_weighted_cost = cost.weighted_cost(&total_cost);
        let mut update_cost = false;
        if let Some(winner) = group_info.winner.as_full_winner() {
            if winner.total_weighted_cost > total_weighted_cost {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            let expr = optimizer.get_expr_memoed(self.expr_id);
            let statistics = cost.derive_statistics(
                &expr.typ,
                &expr.data,
                &input_statistics
                    .iter()
                    .map(|x| x.expect("child winner should always have statistics?"))
                    .collect::<Vec<_>>(),
                Some(RelNodeContext {
                    group_id,
                    expr_id: self.expr_id,
                    children_group_ids: expr.children.clone(),
                }),
                Some(optimizer),
            );
            optimizer.update_group_info(
                group_id,
                GroupInfo {
                    winner: Winner::Full(WinnerInfo {
                        expr_id: self.expr_id,
                        total_weighted_cost,
                        operation_weighted_cost,
                        total_cost,
                        operation_cost,
                        statistics: statistics.into(),
                    }),
                },
            );
        }
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeInputsTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        if self.continue_from.is_none() {
            if optimizer.is_expr_explored(self.expr_id) {
                // skip optimize_inputs to avoid dead-loop: consider join commute being fired twice that produces
                // two projections, therefore having groups like projection1 -> projection2 -> join = projection1.
                trace!(event = "task_skip", task = "optimize_inputs", expr_id = %self.expr_id);
                return Ok(vec![]);
            }
            optimizer.mark_expr_explored(self.expr_id);
        }
        let expr = optimizer.get_expr_memoed(self.expr_id);
        let group_id = optimizer.get_group_id(self.expr_id);
        let children_group_ids = &expr.children;
        let cost = optimizer.cost();

        trace!(event = "task_begin", task = "optimize_inputs", expr_id = %self.expr_id, continue_from = ?self.continue_from, total_children = %children_group_ids.len());

        if let Some(ContinueTask {
            next_group_idx,
            return_from_optimize_group,
        }) = self.continue_from.clone()
        {
            let context = RelNodeContext {
                expr_id: self.expr_id,
                group_id,
                children_group_ids: children_group_ids.clone(),
            };
            let input_statistics = children_group_ids
                .iter()
                .map(|&group_id| {
                    optimizer
                        .get_group_info(group_id)
                        .winner
                        .as_full_winner()
                        .map(|x| x.statistics.clone())
                })
                .collect::<Vec<_>>();
            let input_statistics_ref = input_statistics
                .iter()
                .map(|x| x.as_deref())
                .collect::<Vec<_>>();
            let input_cost = children_group_ids
                .iter()
                .map(|&group_id| {
                    optimizer
                        .get_group_info(group_id)
                        .winner
                        .as_full_winner()
                        .map(|x| x.total_cost.clone())
                        .unwrap_or_else(|| cost.zero())
                })
                .collect::<Vec<_>>();
            let operation_cost = cost.compute_operation_cost(
                &expr.typ,
                &expr.data,
                &input_statistics_ref,
                &input_cost,
                Some(context.clone()),
                Some(optimizer),
            );
            let total_cost = cost.sum(&operation_cost, &input_cost);
            if next_group_idx < children_group_ids.len() {
                let child_group_id = children_group_ids[next_group_idx];
                let group_idx = next_group_idx;
                let group_info = optimizer.get_group_info(child_group_id);
                if !group_info.winner.has_full_winner() {
                    if !return_from_optimize_group {
                        trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, group_idx = %group_idx, yield_to = "optimize_group", optimize_group_id = %child_group_id);
                        return Ok(vec![
                            Box::new(self.continue_from(
                                ContinueTask {
                                    next_group_idx,
                                    return_from_optimize_group: true,
                                },
                                self.pruning,
                            )) as Box<dyn Task<T>>,
                            Box::new(OptimizeGroupTask::new(child_group_id)) as Box<dyn Task<T>>,
                        ]);
                    } else {
                        self.update_winner_impossible(optimizer);
                        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id, "result" = "impossible");
                        return Ok(vec![]);
                    }
                }
                trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, group_idx = %group_idx, yield_to = "next_optimize_input");
                Ok(vec![Box::new(self.continue_from(
                    ContinueTask {
                        next_group_idx: group_idx + 1,
                        return_from_optimize_group: false,
                    },
                    self.pruning,
                )) as Box<dyn Task<T>>])
            } else {
                self.update_winner(input_statistics_ref, operation_cost, total_cost, optimizer);
                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id, "result" = "optimized");
                Ok(vec![])
            }
        } else {
            trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id);
            Ok(vec![Box::new(self.continue_from(
                ContinueTask {
                    next_group_idx: 0,
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
