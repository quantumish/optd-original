use anyhow::Result;
use tracing::trace;
use std::sync::Arc;

use crate::{
    cascades::{
        memo::{RelMemoNode, SubGroupInfo, Winner},
        optimizer::ExprId,
        tasks::OptimizeGroupTask,
        CascadesOptimizer, GroupId, RelNodeContext, SubGroupId
    }, cost::Cost, physical_prop::PhysicalPropsBuilder, rel_node::RelNodeTyp
};

use super::Task;

#[derive(Debug, Clone)]
struct ContinueTask {
    next_group_idx: usize,
    input_cost: Vec<Cost>,
    return_from_optimize_group: bool,
}

/// OptimizeInputsTask calls OptimizeGroupTask for each child of the current expression.
/// It is the only task that move expressions to sub groups from the default subgroup.
/// 
/// If there's no required physical props(PhysicalProps::Any), it only updates the winner
///     in the default subgroup. 
/// 
/// If there's required physical props, 
/// 1. After pass child physical properties, it update the winner in the default sub group first
/// 2. it then create the counterpart expr which satisfy required physical props, and move it to subgroup
pub struct OptimizeInputsTask<T: RelNodeTyp, P: PhysicalPropsBuilder<T>> {
    expr_id: ExprId,
    continue_from: Option<ContinueTask>,
    pruning: bool,
    physical_props_builder: Arc<P>,
    required_physical_props: P::PhysicalProps,
    required_children_props: Option<Vec<P::PhysicalProps>>,
    pass_to_children_props: Option<P::PhysicalProps>,
    required_enforce_props: Option<P::PhysicalProps>,
}

impl<T: RelNodeTyp, P:PhysicalPropsBuilder<T>> OptimizeInputsTask<T, P> {
    pub fn new(expr_id: ExprId, pruning: bool, physical_props_builder: Arc<P>, required_physical_props: P::PhysicalProps) -> Self {
        Self {
            expr_id,
            continue_from: None,
            pruning,
            physical_props_builder,
            required_physical_props,
            required_children_props: None,
            pass_to_children_props: None,
            required_enforce_props: None
        }
    }

    fn continue_from(&self, cont: ContinueTask, pruning: bool) -> Self {
        Self {
            expr_id: self.expr_id,
            continue_from: Some(cont),
            pruning,
            physical_props_builder: self.physical_props_builder.clone(),
            required_physical_props: self.required_physical_props.clone(),
            required_children_props: self.required_children_props.clone(),
            pass_to_children_props: self.pass_to_children_props.clone(),
            required_enforce_props: self.required_enforce_props.clone()
        }
    }

    /// first invoke of this task, compute the cost of children
    fn first_invoke(
        &self,
        children: &[(GroupId, SubGroupId)],
        required_children_props: &Vec<P::PhysicalProps>,
        optimizer: &mut CascadesOptimizer<T, P>,
    ) -> Vec<Cost> {
        let zero_cost = optimizer.cost().zero();
        let mut input_cost = Vec::with_capacity(children.len());
        for (&child, &ref prop) in children.iter().zip(required_children_props.iter()) {
            // when optimize input task is first invoked, all the children are in default subgroup
            assert!(child.1 == SubGroupId(0));
            let group = optimizer.get_sub_group_info_by_props(child.0, &prop);
            if let Some(group) = group {
                if group.winner.is_some() {
                    let winner = group.winner.unwrap();
                    if !winner.impossible {
                        // the full winner case
                        input_cost.push(winner.cost.clone());
                        continue;
                    }
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

    fn update_winner(
        &self,
        cost_so_far: &Cost,
        optimizer: &mut CascadesOptimizer<T, P>,
        physical_prop: Option<P::PhysicalProps>,
        expr_id: Option<ExprId>,
    ) {
        let group_id = optimizer.get_group_id(self.expr_id);
        let group_info = optimizer.get_group_info(group_id);
        let mut update_cost = false;
        if let Some(ref winner) = group_info.winner {
            if winner.impossible || &winner.cost > cost_so_far {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            if physical_prop.is_some() {
                optimizer.update_sub_group_info(
                    group_id,
                    expr_id, 
                    SubGroupInfo {
                        winner: Some(Winner {
                            impossible: false,
                            expr_id: expr_id.unwrap(),
                            cost: cost_so_far.clone(),
                        }),
                    },
                    &physical_prop.unwrap(),
                );
                return;
            } 

            optimizer.update_group_info(
                group_id,
                SubGroupInfo {
                    winner: Some(Winner {
                        impossible: false,
                        expr_id: self.expr_id,
                        cost: cost_so_far.clone(),
                    }),
                },
            );
        }
    }

    fn create_counterpart_expr(&self, optimizer: &mut CascadesOptimizer<T, P>, expr: Arc<RelMemoNode<T>>) -> ExprId{
        let children_group_ids = &expr.children;
        let mut changed = false;
        let mut new_children_group_ids = Vec::with_capacity(children_group_ids.len());
        for (group_id, required_props) in children_group_ids.iter().zip(self.required_children_props.clone().unwrap().iter()){
            let group_id = group_id.0;
            let sub_group_id = optimizer.get_sub_group_id(group_id, &required_props).unwrap();
            if sub_group_id.0 != 0{
                changed = true;
            }
            new_children_group_ids.push((group_id, sub_group_id));
        }
        if changed {
            let new_expr = RelMemoNode {
                typ: expr.typ.clone(),
                data: expr.data.clone(),
                children: new_children_group_ids,
            };
            let group_id = optimizer.get_group_id(self.expr_id);
            // add new expr to sub group
            return optimizer.add_sub_group_expr(new_expr, group_id, self.pass_to_children_props.as_ref().unwrap());
        }
        self.expr_id
    }
}

impl<T: RelNodeTyp, P:PhysicalPropsBuilder<T>> Task<T,P> for OptimizeInputsTask<T, P> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T,P>) -> Result<Vec<Box<dyn Task<T,P>>>> {
        if optimizer.tasks.iter().any(|t| {
            if let Some(task) = t.as_any().downcast_ref::<Self>() {
                task.expr_id == self.expr_id 
                    && task.required_physical_props == self.required_physical_props
                    && task.required_children_props == self.required_children_props
                    && task.required_enforce_props == self.required_enforce_props
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
                // all the expr for OptimizeInputTask are come from default subgroup, their children point to default sub group id 
                // we don't need the children subgroup id then
                // instead, we use the required_children_props to get the children sub group info
                let group_id = children_group_ids[next_group_idx].0;
                let group_idx = next_group_idx;
                let required_child_physical_props = &<std::option::Option<Vec<<P as PhysicalPropsBuilder<T>>::PhysicalProps>> as Clone>::clone(&self.required_children_props).unwrap()[group_idx];
                let sub_group_info = optimizer.get_sub_group_info_by_props(group_id, &required_child_physical_props);
                let mut has_full_winner = false;
                if let Some(sub_group_info) = sub_group_info {
                    if sub_group_info.winner.is_some(){
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
                            )) as Box<dyn Task<T,P>>,
                            Box::new(OptimizeGroupTask::new(group_id, self.physical_props_builder.clone(), required_child_physical_props.clone())) as Box<dyn Task<T,P>>,
                        ]);
                    } else {
                        let sub_group_info = optimizer.get_sub_group_info_by_props(group_id, &required_child_physical_props);
                        if let Some(sub_group_info) = sub_group_info{
                            if sub_group_info.winner.is_some(){
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
                                        },
                                        required_child_physical_props,
                                    );
                                    trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                                    return Ok(vec![]);
                                }
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
                            },
                            required_child_physical_props,
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
                )) as Box<dyn Task<T,P>>])
            } else {
                let cost_so_far = cost.sum(
                        &cost.compute_cost(
                                &expr.typ,
                                    &expr.data,
                            &input_cost,
                            Some(context.clone()),
                            Some(optimizer),
                        ),
                        &input_cost,
                    );
                // 1. finish optimizing all the children, let's update the winner for the default sub group first
                self.update_winner(
                    &cost_so_far,
                    optimizer,
                    None,
                    None
                );

                // 2. create counterpart expr based on required child physical prop
                //   which will create a sub group with pass_to_child_props in current group
                //   having children sub groups satifying required children physical prop
                let mut sub_group_id = SubGroupId(0);
                if self.pass_to_children_props.is_some(){
                    let pass_to_children_props = self.pass_to_children_props.clone().unwrap();
                    if !self.physical_props_builder.is_any(&pass_to_children_props){
                        let counterpart_expr_id = self.create_counterpart_expr(optimizer, expr);
                        sub_group_id = optimizer.get_sub_group_id(group_id, &pass_to_children_props).unwrap();
                        self.update_winner(
                            &cost_so_far,
                            optimizer,
                            Some(pass_to_children_props),
                            Some(counterpart_expr_id)
                        );
                    }
                }

                // 3. start enforcer task to enforce the required physical props
                if self.required_enforce_props.is_some() {
                    let required_enforcer_props = self.required_enforce_props.clone().unwrap();
                    if !self.physical_props_builder.is_any(&required_enforcer_props){
                        // enforce start enforce operator based on the winner of (group_id, sub_group_id)
                        let winner_info  = optimizer.get_sub_group_info_by_id(group_id, sub_group_id).winner.unwrap();
                        let winner_expr_id = winner_info.expr_id;
                        let winner_cost = winner_info.cost;

                        let winner_expr = optimizer.get_all_expr_bindings(winner_expr_id, Some(0));
                        assert!(winner_expr.len() == 1);
                        let winner_expr = winner_expr.get(0).unwrap();

                        // TODO: we might need to add a match and pick here to create RelNodeRef based on RelMemoNodeRef
                        let new_expr = self.physical_props_builder.enforce(winner_expr.clone(), &required_enforcer_props);
                        
                        let enforcer_cost = cost.sum(&cost.compute_cost(
                            &new_expr.typ,
                                &new_expr.data,
                        &[winner_cost.clone()],
                        Some(context.clone()),
                        Some(optimizer),
                        ),
                        &[winner_cost]);
                        
                        let new_expr_memo = RelMemoNode {
                            typ: new_expr.typ.clone(),
                            data: new_expr.data.clone(),
                            children: vec![(group_id, sub_group_id)],
                        };
                        // here we use required_physical_props because the base expr provides the pass_to_children_props and enforcer provides the required_enforce_props
                        // they together provides the required_physical_props
                        let new_expr_id = optimizer.add_sub_group_expr(new_expr_memo, group_id, &self.required_physical_props);
                        self.update_winner(
                            &enforcer_cost,
                            optimizer,
                            Some(self.required_physical_props.clone()),
                            Some(new_expr_id)
                        );
                    }
                }

                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                Ok(vec![])
            }
        } else {
            // 1. if there's no required physical props, we make pass_to_children_props as any and required_enforce_props as any
            if self.physical_props_builder.is_any(&self.required_physical_props){
                let pass_to_children_props = Some(self.physical_props_builder.any());
                let required_enforce_props = Some(self.physical_props_builder.any());
                let required_children_props = vec![self.physical_props_builder.any(); children_group_ids.len()];
                let input_cost = self.first_invoke(children_group_ids,  &required_children_props, optimizer);
                trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id);
                return Ok(vec![Box::new(OptimizeInputsTask::<T,P>{
                    expr_id: self.expr_id,
                    continue_from: Some(ContinueTask {
                        next_group_idx: 0,
                        input_cost,
                        return_from_optimize_group: false,
                    }),
                    pruning: self.pruning,
                    physical_props_builder: self.physical_props_builder.clone(),
                    required_physical_props: self.required_physical_props.clone(),
                    required_enforce_props: required_enforce_props,
                    required_children_props: Some(required_children_props),
                    pass_to_children_props: pass_to_children_props,
                }) as Box<dyn Task<T,P>>]);
            }

            // separate the physical properties for the current expr gives us a vector of (pass_to_children_props, required_enforce_props, required_children_props)
            // 1. for situation that current expr cannot provide any of the required physical props, we set others as any and put all required to required_enforce_props
            // 2. for situation that expr can pass requirement to children, we separate required_props to pass_to_children_props and required_enforce_props
            // 3. for situation that expr can provide the required physical props by its own(sort merge join to provide ordering), we set pass_to_children_props to any and required_enforce_props to any
            let props = self.physical_props_builder.separate_physical_props(&expr.typ, &expr.data, &self.required_physical_props, children_group_ids.len());

            let mut tasks = Vec::with_capacity(props.len());
            for (pass_to_children_props, required_enforce_prop, required_children_props) in props.into_iter(){
                let input_cost = self.first_invoke(children_group_ids,  &required_children_props, optimizer);
                tasks.push(
                        Box::new(OptimizeInputsTask::<T,P>{
                            expr_id: self.expr_id,
                            continue_from: Some(ContinueTask{
                                next_group_idx: 0,
                                input_cost: input_cost,
                                return_from_optimize_group: false
                            }),
                            pruning: self.pruning,
                            physical_props_builder: self.physical_props_builder.clone(),
                            required_physical_props: self.required_physical_props.clone(),
                            required_enforce_props: Some(required_enforce_prop),
                            required_children_props: Some(required_children_props),
                            pass_to_children_props: Some(pass_to_children_props), // Add a semicolon here
                        }) as Box<dyn Task<T,P>>
                    );
            }
            
            trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id);
            Ok(tasks)
        }
    }

    fn describe(&self) -> String {
        format!("optimize_inputs {}", self.expr_id)
    }
}
