use tracing::trace;

use crate::{
    cascades::{
        memo::{GroupInfo, Winner, WinnerInfo},
        optimizer::{ExprId, RelNodeContext},
        CascadesOptimizer, GroupId, Memo,
    },
    cost::{Cost, Statistics},
    nodes::{ArcPredNode, NodeType},
};

use super::{optimize_group::OptimizeGroupTask, Task};

pub struct OptimizeInputsTask {
    parent_task_id: Option<usize>,
    task_id: usize,
    expr_id: ExprId,
    cost_limit: Option<isize>,
    iteration: usize,
}

impl OptimizeInputsTask {
    pub fn new(
        parent_task_id: Option<usize>,
        task_id: usize,
        expr_id: ExprId,
        cost_limit: Option<isize>,
    ) -> Self {
        Self {
            parent_task_id,
            task_id,
            expr_id,
            cost_limit,
            iteration: 0,
        }
    }

    fn new_continue_iteration<T: NodeType>(
        &self,
        optimizer: &CascadesOptimizer<T, impl Memo<T>>,
    ) -> Self {
        Self {
            parent_task_id: Some(self.task_id),
            task_id: optimizer.get_next_task_id(),
            expr_id: self.expr_id,
            cost_limit: self.cost_limit,
            iteration: self.iteration + 1,
        }
    }
}

fn get_input_cost<T: NodeType, M: Memo<T>>(
    children: &[GroupId],
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<Cost> {
    let cost = optimizer.cost();
    let input_cost = children
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
    input_cost
}

// TODO: Horrific mess
fn update_winner<T: NodeType>(expr_id: ExprId, optimizer: &CascadesOptimizer<T, impl Memo<T>>) {
    let cost = optimizer.cost();
    let expr = optimizer.get_expr_memoed(expr_id);
    let group_id = optimizer.get_group_id(expr_id);

    // Calculate cost
    let context = RelNodeContext {
        expr_id,
        group_id,
        children_group_ids: expr.children.clone(),
    };
    let input_cost = get_input_cost(&expr.children, optimizer);
    let input_statistics = context
        .children_group_ids
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
    let input_statistics_refs: Vec<&Statistics> = input_statistics
        .iter()
        .map(|x| {
            x.as_ref()
                .expect("child winner should always have statistics?")
                .as_ref()
        })
        .collect();
    let predicates: Vec<ArcPredNode<T>> = expr
        .predicates
        .iter()
        .map(|&pred_id| optimizer.get_predicate_binding(pred_id))
        .collect();
    let statistics = cost.derive_statistics(
        &expr.typ,
        &input_statistics_refs,
        &predicates,
        Some(RelNodeContext {
            group_id,
            expr_id,
            children_group_ids: expr.children.clone(),
        }),
        Some(optimizer),
    );
    let materialized_predicates = expr
        .predicates
        .iter()
        .map(|x| optimizer.get_predicate_binding(*x))
        .collect::<Vec<_>>();
    let operation_cost = cost.compute_operation_cost(
        &expr.typ,
        &input_statistics_ref,
        &materialized_predicates,
        &input_cost,
        Some(context.clone()),
        Some(optimizer),
    );

    let total_cost = cost.sum(&operation_cost, &input_cost);

    let operation_weighted_cost = cost.weighted_cost(&operation_cost);
    let total_weighted_cost = cost.weighted_cost(&total_cost);

    let group_id = optimizer.get_group_id(expr_id);
    let group_info = optimizer.get_group_info(group_id);

    // Update best cost for group if desired
    let mut update_cost = false;
    if let Some(winner) = group_info.winner.as_full_winner() {
        if winner.total_weighted_cost > total_weighted_cost {
            update_cost = true;
        }
    } else {
        update_cost = true;
    }
    // TODO: Deciding the winner and constructing the struct should
    // be performed in the memotable
    if update_cost {
        optimizer.update_group_info(
            group_id,
            GroupInfo {
                winner: Winner::Full(WinnerInfo {
                    expr_id,
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

/// TODO
///
/// Pseudocode:
/// function OptInputs(expr, rule, limit)
///     childExpr ← expr.GetNextInput()
///     if childExpr is null then
///         memo.UpdateBestPlan(expr)
///         return
///     tasks.Push(OptInputs(expr, limit))
///     UpdateCostBound(expr)
///     limit ← UpdateCostLimit(expr, limit)
///     tasks.Push(OptGrp(GetGroup(childExpr), limit))
impl<T: NodeType, M: Memo<T>> Task<T, M> for OptimizeInputsTask {
    fn execute(&self, optimizer: &CascadesOptimizer<T, M>) {
        let expr = optimizer.get_expr_memoed(self.expr_id);
        let group_id = optimizer.get_group_id(self.expr_id);
        // TODO: add typ to more traces and iteration to traces below
        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_begin", task = "optimize_inputs", iteration = %self.iteration, group_id = %group_id, expr_id = %self.expr_id, expr = %expr);
        let next_child_expr = expr.children.get(self.iteration);
        if let None = next_child_expr {
            // TODO: If we want to support interrupting the optimizer, it might
            // behoove us to update the winner more often than this.
            update_winner(self.expr_id, optimizer);
            trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_finish", task = "optimize_inputs", iteration = %self.iteration, group_id = %group_id, expr_id = %self.expr_id, expr = %expr);
            return;
        }
        let next_child_expr = next_child_expr.unwrap();

        //TODO(parallel): Task dependency
        //TODO: Should be able to add multiple tasks at once
        optimizer.push_task(Box::new(self.new_continue_iteration(optimizer)));
        // TODO updatecostbound (involves cost limit)
        let new_limit = None; // TODO: How do we update cost limit
        optimizer.push_task(Box::new(OptimizeGroupTask::new(
            Some(self.task_id),
            optimizer.get_next_task_id(),
            *next_child_expr,
            new_limit,
        )));
        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_finish", task = "optimize_inputs", iteration = %self.iteration, group_id = %group_id, expr_id = %self.expr_id, expr = %expr);
    }
}
