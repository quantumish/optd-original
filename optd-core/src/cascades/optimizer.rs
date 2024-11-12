// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tracing::trace;

use super::memo::{ArcMemoPlanNode, GroupInfo, Memo};
use super::tasks::OptimizeGroupTask;
use super::{NaiveMemo, Task};
use crate::cascades::memo::Winner;
use crate::cascades::tasks::get_initial_task;
use crate::cost::CostModel;
use crate::nodes::{
    ArcPlanNode, ArcPredNode, NodeType, PlanNodeMeta, PlanNodeMetaMap, PlanNodeOrGroup,
};
use crate::optimizer::Optimizer;
use crate::property::{PropertyBuilder, PropertyBuilderAny};
use crate::rules::{Rule, RuleMatcher};

pub type RuleId = usize;

#[derive(Default, Clone, Debug)]
pub struct OptimizerContext {
    pub budget_used: bool,
    pub rules_applied: usize,
}

#[derive(Default, Clone, Debug)]
pub struct OptimizerProperties {
    pub panic_on_budget: bool,
    /// If the number of rules applied exceeds this number, we stop applying logical rules.
    pub partial_explore_iter: Option<usize>,
    /// Plan space can be expanded by this number of times before we stop applying logical rules.
    pub partial_explore_space: Option<usize>,
    /// Disable pruning during optimization.
    pub disable_pruning: bool,
}

pub struct CascadesOptimizer<T: NodeType, M: Memo<T> = NaiveMemo<T>> {
    memo: M,
    /// Stack of tasks that are waiting to be executed
    tasks: Vec<Box<dyn Task<T, M>>>,
    /// Monotonically increasing counter for task invocations
    task_counter: AtomicUsize,
    explored_group: HashSet<GroupId>,
    explored_expr: HashSet<ExprId>,
    applied_rules: HashMap<ExprId, HashSet<RuleId>>,
    /// Transformation rules that may be used while exploring
    /// (logical -> logical)
    transformation_rules: Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]>,
    /// Implementation rules that may be used while optimizing
    /// (logical -> physical)
    implementation_rules: Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]>,
    disabled_rules: HashSet<usize>,
    cost: Arc<dyn CostModel<T, M>>,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    pub ctx: OptimizerContext,
    pub prop: OptimizerProperties,
}

/// `RelNode` only contains the representation of the plan nodes. Sometimes, we need more context,
/// i.e., group id and expr id, during the optimization phase. All these information are collected
/// in this struct.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct RelNodeContext {
    pub group_id: GroupId,
    pub expr_id: ExprId,
    pub children_group_ids: Vec<GroupId>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct GroupId(pub(super) usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct ExprId(pub usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct PredId(pub usize);

impl Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "!{}", self.0)
    }
}

impl Display for ExprId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for PredId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "P{}", self.0)
    }
}

impl<T: NodeType> CascadesOptimizer<T, NaiveMemo<T>> {
    pub fn new(
        transformation_rules: Arc<[Arc<dyn Rule<T, Self>>]>,
        implementation_rules: Arc<[Arc<dyn Rule<T, Self>>]>,
        cost: Box<dyn CostModel<T, NaiveMemo<T>>>,
        property_builders: Vec<Box<dyn PropertyBuilderAny<T>>>,
    ) -> Self {
        Self::new_with_prop(
            transformation_rules,
            implementation_rules,
            cost,
            property_builders,
            Default::default(),
        )
    }

    pub fn new_with_prop(
        transformation_rules: Arc<[Arc<dyn Rule<T, Self>>]>,
        implementation_rules: Arc<[Arc<dyn Rule<T, Self>>]>,
        cost: Box<dyn CostModel<T, NaiveMemo<T>>>,
        property_builders: Vec<Box<dyn PropertyBuilderAny<T>>>,
        prop: OptimizerProperties,
    ) -> Self {
        let tasks = Vec::new();
        // Assign rule IDs
        let transformation_rules: Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]> = transformation_rules
            .into_iter()
            .enumerate()
            .map(|(i, r)| (i, r.clone()))
            .collect();
        let implementation_rules: Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]> = implementation_rules
            .into_iter()
            .enumerate()
            .map(|(i, r)| (i + transformation_rules.len(), r.clone()))
            .collect();
        debug_assert!(transformation_rules.iter().all(|(_, r)| !r.is_impl_rule()));
        debug_assert!(implementation_rules.iter().all(|(_, r)| r.is_impl_rule()));
        let property_builders: Arc<[_]> = property_builders.into();
        let memo = NaiveMemo::new(property_builders.clone());
        Self {
            memo,
            task_counter: AtomicUsize::new(0),
            tasks,
            explored_group: HashSet::new(),
            explored_expr: HashSet::new(),
            applied_rules: HashMap::new(),
            transformation_rules,
            implementation_rules,
            cost: cost.into(),
            ctx: OptimizerContext::default(),
            property_builders,
            prop,
            disabled_rules: HashSet::new(),
        }
    }

    /// Clear the memo table and all optimizer states.
    pub fn step_clear(&mut self) {
        self.memo = NaiveMemo::new(self.property_builders.clone());
        self.applied_rules.clear();
        self.explored_group.clear();
        self.explored_expr.clear();
    }

    /// Clear the winner so that the optimizer can continue to explore the group.
    pub fn step_clear_winner(&mut self) {
        self.memo.clear_winner();
        self.explored_expr.clear();
    }
}

impl<T: NodeType, M: Memo<T>> CascadesOptimizer<T, M> {
    pub fn panic_on_explore_limit(&mut self, enabled: bool) {
        self.prop.panic_on_budget = enabled;
    }

    pub fn disable_pruning(&mut self, enabled: bool) {
        self.prop.disable_pruning = enabled;
    }

    pub fn cost(&self) -> Arc<dyn CostModel<T, M>> {
        self.cost.clone()
    }

    pub fn transformation_rules(&self) -> Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]> {
        self.transformation_rules.clone()
    }

    pub fn implementation_rules(&self) -> Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]> {
        self.implementation_rules.clone()
    }

    pub fn disable_rule(&mut self, rule_id: usize) {
        self.disabled_rules.insert(rule_id);
    }

    pub fn enable_rule(&mut self, rule_id: usize) {
        self.disabled_rules.remove(&rule_id);
    }

    pub fn is_rule_disabled(&self, rule_id: usize) -> bool {
        self.disabled_rules.contains(&rule_id)
    }

    pub fn dump(&self) {
        for group_id in self.memo.get_all_group_ids() {
            let winner_str = match &self.memo.get_group_info(group_id).winner {
                Winner::Impossible => "winner=<impossible>".to_string(),
                Winner::Unknown => "winner=<unknown>".to_string(),
                Winner::Full(winner) => {
                    let expr = self.memo.get_expr_memoed(winner.expr_id);
                    format!(
                        "winner={} weighted_cost={} cost={} stat={} | {}",
                        winner.expr_id,
                        winner.total_weighted_cost,
                        self.cost.explain_cost(&winner.total_cost),
                        self.cost.explain_statistics(&winner.statistics),
                        expr
                    )
                }
            };
            println!("group_id={} {}", group_id, winner_str);
            let group = self.memo.get_group(group_id);
            for (id, property) in self.property_builders.iter().enumerate() {
                println!(
                    "  {}={}",
                    property.property_name(),
                    property.display(group.properties[id].as_ref())
                )
            }
            if let Some(predicate_binding) = self.memo.try_get_predicate_binding(group_id) {
                println!("  predicate={}", predicate_binding);
            }
            let mut all_predicates = BTreeSet::new();
            for expr_id in self.memo.get_all_exprs_in_group(group_id) {
                let memo_node = self.memo.get_expr_memoed(expr_id);
                for pred in &memo_node.predicates {
                    all_predicates.insert(*pred);
                }
                println!("  expr_id={} | {}", expr_id, memo_node);
            }
            for pred in all_predicates {
                println!("  {}={}", pred, self.memo.get_pred(pred));
            }
        }
    }

    /// Optimize a `RelNode`.
    pub fn step_optimize_rel(&mut self, root_rel: ArcPlanNode<T>) -> Result<GroupId> {
        let (group_id, _) = self.add_new_expr(root_rel);
        self.fire_optimize_tasks(group_id);
        Ok(group_id)
    }

    /// Gets the group binding.
    pub fn step_get_optimize_rel(
        &self,
        group_id: GroupId,
        meta: &mut Option<PlanNodeMetaMap>,
    ) -> Result<ArcPlanNode<T>> {
        let res = self
            .memo
            .get_best_group_binding(group_id, |node, group_id, info| {
                if let Some(meta) = meta {
                    let node = node.as_ref() as *const _ as usize;
                    let node_meta = PlanNodeMeta::new(
                        group_id,
                        info.total_weighted_cost,
                        info.total_cost.clone(),
                        info.statistics.clone(),
                        self.cost.explain_cost(&info.total_cost),
                        self.cost.explain_statistics(&info.statistics),
                    );
                    meta.insert(node, node_meta);
                }
            });
        if res.is_err() && cfg!(debug_assertions) {
            self.dump();
        }
        res
    }

    pub fn get_next_task_id(&self) -> usize {
        self.task_counter.fetch_add(1, Ordering::AcqRel)
    }

    pub fn push_task(&mut self, task: Box<dyn Task<T, M>>) {
        self.tasks.push(task);
    }

    fn pop_task(&mut self) -> Option<Box<dyn Task<T, M>>> {
        self.tasks.pop()
    }

    fn fire_optimize_tasks(&mut self, root_group_id: GroupId) {
        trace!(event = "fire_optimize_tasks", root_group_id = %root_group_id);
        let initial_task_id = self.get_next_task_id();
        self.push_task(get_initial_task(initial_task_id, root_group_id));
        // get the task from the stack
        self.ctx.budget_used = false;
        let plan_space_begin = self.memo.estimated_plan_space();
        let mut iter = 0;
        while let Some(task) = self.pop_task() {
            task.execute(self);

            // TODO: Iter is wrong
            iter += 1;
            if !self.ctx.budget_used {
                let plan_space = self.memo.estimated_plan_space();
                if let Some(partial_explore_space) = self.prop.partial_explore_space {
                    if plan_space - plan_space_begin > partial_explore_space {
                        println!(
                            "plan space size budget used, not applying logical rules any more. current plan space: {}",
                            plan_space
                        );
                        self.ctx.budget_used = true;
                        if self.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                } else if let Some(partial_explore_iter) = self.prop.partial_explore_iter {
                    if iter >= partial_explore_iter {
                        println!(
                            "plan explore iter budget used, not applying logical rules any more. current plan space: {}",
                            plan_space
                        );
                        self.ctx.budget_used = true;
                        if self.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                }
            }
        }
    }

    fn optimize_inner(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        let (group_id, _) = self.add_new_expr(root_rel);
        self.fire_optimize_tasks(group_id);
        self.memo.get_best_group_binding(group_id, |_, _, _| {})
    }

    pub fn resolve_group_id(&self, root_rel: PlanNodeOrGroup<T>) -> GroupId {
        root_rel.unwrap_group()
    }

    pub(super) fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        self.memo.get_all_exprs_in_group(group_id)
    }

    pub fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        self.memo.add_new_expr(rel_node)
    }

    pub fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        self.memo.add_expr_to_group(rel_node, group_id)
    }

    pub(super) fn get_group_info(&self, group_id: GroupId) -> &GroupInfo {
        self.memo.get_group_info(group_id)
    }

    pub(super) fn update_group_info(&mut self, group_id: GroupId, group_info: GroupInfo) {
        self.memo.update_group_info(group_id, group_info)
    }

    /// Get the properties of a Cascades group
    /// P is the type of the property you expect
    /// idx is the idx of the property you want. The order of properties is defined
    ///   by the property_builders parameter in CascadesOptimizer::new()
    pub fn get_property_by_group<P: PropertyBuilder<T>>(
        &self,
        group_id: GroupId,
        idx: usize,
    ) -> P::Prop {
        self.memo.get_group(group_id).properties[idx]
            .downcast_ref::<P::Prop>()
            .unwrap()
            .clone()
    }

    pub(super) fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        self.memo.get_group_id(expr_id)
    }

    pub(super) fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T> {
        self.memo.get_expr_memoed(expr_id)
    }

    pub fn get_predicate_binding(&self, group_id: GroupId) -> Option<ArcPlanNode<T>> {
        self.memo.get_predicate_binding(group_id)
    }

    pub fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T> {
        self.memo.get_pred(pred_id)
    }

    pub(super) fn is_group_explored(&self, group_id: GroupId) -> bool {
        self.explored_group.contains(&group_id)
    }

    pub(super) fn mark_group_explored(&mut self, group_id: GroupId) {
        self.explored_group.insert(group_id);
    }

    pub(super) fn is_expr_explored(&self, expr_id: ExprId) -> bool {
        self.explored_expr.contains(&expr_id)
    }

    pub(super) fn mark_expr_explored(&mut self, expr_id: ExprId) {
        self.explored_expr.insert(expr_id);
    }

    pub(super) fn unmark_expr_explored(&mut self, expr_id: ExprId) {
        self.explored_expr.remove(&expr_id);
    }

    pub(super) fn is_rule_applied(&self, group_expr_id: ExprId, rule_id: RuleId) -> bool {
        self.applied_rules
            .get(&group_expr_id)
            .map(|rules| rules.contains(&rule_id))
            .unwrap_or(false)
    }

    pub(super) fn mark_rule_applied(&mut self, group_expr_id: ExprId, rule_id: RuleId) {
        self.applied_rules
            .entry(group_expr_id)
            .or_default()
            .insert(rule_id);
    }

    pub fn memo(&self) -> &M {
        &self.memo
    }
}

impl<T: NodeType, M: Memo<T>> Optimizer<T> for CascadesOptimizer<T, M> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        self.optimize_inner(root_rel)
    }

    fn get_property<P: PropertyBuilder<T>>(
        &self,
        root_rel: PlanNodeOrGroup<T>,
        idx: usize,
    ) -> P::Prop {
        self.get_property_by_group::<P>(self.resolve_group_id(root_rel), idx)
    }
}

pub fn rule_matches_expr<T: NodeType, M: Memo<T>>(
    rule: &Arc<dyn Rule<T, CascadesOptimizer<T, M>>>,
    expr: &ArcMemoPlanNode<T>,
) -> bool {
    let matcher = rule.matcher();
    let typ_to_match = &expr.typ;
    match matcher {
        RuleMatcher::MatchNode { typ, .. } => typ == typ_to_match,
        RuleMatcher::MatchDiscriminant {
            typ_discriminant, ..
        } => *typ_discriminant == std::mem::discriminant(typ_to_match),
        _ => panic!("IR should have root node of match"), // TODO: what does this mean? replace text
    }
}
