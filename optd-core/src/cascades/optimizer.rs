use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
};

use anyhow::Result;

use crate::{
    cost::CostModel,
    optimizer::Optimizer,
    property::{PropertyBuilder, PropertyBuilderAny},
    rel_node::{RelNodeMeta, RelNodeRef, RelNodeTyp},
    rules::{Rule, RuleMatcher},
};

use super::{
    memo::{GroupInfo, Memo, RelMemoNodeRef},
    tasks::{get_initial_task, Task},
};

/// `RelNode` only contains the representation of the plan nodes. Sometimes, we need more context, i.e., group id and
/// expr id, during the optimization phase. All these information are collected in this struct.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct RelNodeContext {
    pub group_id: GroupId,
    pub expr_id: ExprId,
    pub children_group_ids: Vec<GroupId>,
}

// TODO: can these be somewhere else?
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct GroupId(pub(super) usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct ExprId(pub usize);

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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct RuleId(pub usize);

impl Display for RuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "R{}", self.0)
    }
}

impl From<usize> for RuleId {
    fn from(id: usize) -> Self {
        Self(id)
    }
}

/// TODO: Docs
struct CascadesOptimizerState<T: RelNodeTyp> {
    pub memo: Memo<T>,
    pub explored_groups: HashSet<GroupId>, // TODO: Should we move this information into the memo groupinfo?? (I think yes)
    pub applied_rules: HashMap<ExprId, HashSet<RuleId>>, // TODO: Should this info be in the memo also?
    pub disabled_rules: HashSet<RuleId>,
}

/// TODO: Docs
pub struct CascadesOptimizer<T: RelNodeTyp> {
    /// Tasks that are waiting to be executed
    tasks: Mutex<VecDeque<Box<dyn Task<T>>>>,
    /// Parts of the internal state of the optimizer, behind a RwLock
    state: RwLock<CascadesOptimizerState<T>>,
    /// Number of transformation rule applications that have ocurred thus far.
    /// We can use this to terminate exploration early.
    transformation_count: AtomicUsize,
    /// Transformation rules that may be used while exploring
    /// (logical -> logical)
    transformation_rules: Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]>,
    /// Implementation rules that may be used while optimizing
    /// (logical -> physical)
    implementation_rules: Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]>,
    /// Property builders which may be used to derive properties
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    /// Cost model, used to determine the cost of a given plan
    cost: Arc<dyn CostModel<T>>,
}

impl<T: RelNodeTyp> CascadesOptimizer<T> {
    /// Create a new CascadesOptimizer object
    pub fn new(
        transformation_rules: Arc<[Arc<dyn Rule<T, Self>>]>,
        implementation_rules: Arc<[Arc<dyn Rule<T, Self>>]>,
        cost: Arc<dyn CostModel<T>>,
        property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    ) -> Self {
        // Assign rule IDs
        let transformation_rules: Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]> = transformation_rules
            .into_iter()
            .enumerate()
            .map(|(i, r)| (RuleId(i), r.clone()))
            .collect();
        let implementation_rules = implementation_rules
            .into_iter()
            .enumerate()
            .map(|(i, r)| (RuleId(i + transformation_rules.len()), r.clone()))
            .collect();

        // Create struct instance
        Self {
            tasks: Mutex::default(),
            state: RwLock::new(CascadesOptimizerState {
                memo: Memo::new(property_builders.clone()),
                explored_groups: HashSet::new(),
                applied_rules: HashMap::new(),
                disabled_rules: HashSet::new(),
            }),
            transformation_count: AtomicUsize::new(0),
            transformation_rules,
            implementation_rules,
            property_builders,
            cost,
        }
    }

    pub fn enqueue_task(&self, task: Box<dyn Task<T>>) {
        self.tasks.lock().unwrap().push_back(task);
    }

    fn dequeue_task(&self) -> Option<Box<dyn Task<T>>> {
        self.tasks.lock().unwrap().pop_front()
    }

    /// Returns if a given group ID has already been explored in this run
    pub fn is_group_explored(&self, group_id: GroupId) -> bool {
        self.state
            .read()
            .unwrap()
            .explored_groups
            .contains(&group_id)
    }

    /// Marks a given group ID as having already been explored in this run
    pub fn mark_group_explored(&self, group_id: GroupId) {
        self.state.write().unwrap().explored_groups.insert(group_id);
    }

    pub fn get_all_group_bindings(
        &self,
        group_id: GroupId,
        physical_only: bool,
    ) -> Vec<RelNodeRef<T>> {
        self.state.read().unwrap().memo.get_all_group_bindings(
            group_id,
            physical_only,
            true,
            Some(10),
        )
    }

    pub fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        self.state
            .read()
            .unwrap()
            .memo
            .get_all_exprs_in_group(group_id)
    }

    pub fn get_all_expr_bindings(
        &self,
        expr_id: ExprId,
        level: Option<usize>,
    ) -> Vec<RelNodeRef<T>> {
        // TODO: expr_bindings is not descriptive
        // Additionally, arguments (to this and memo table) are not easy to understand
        self.state
            .read()
            .unwrap()
            .memo
            .get_all_expr_bindings(expr_id, false, false, level)
    }

    pub fn get_expr_memoed(&self, expr_id: ExprId) -> RelMemoNodeRef<T> {
        self.state.read().unwrap().memo.get_expr_memoed(expr_id)
    }

    pub fn transformation_rules(&self) -> &Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]> {
        &self.transformation_rules
    }

    pub fn implementation_rules(&self) -> &Arc<[(RuleId, Arc<dyn Rule<T, Self>>)]> {
        &self.implementation_rules
    }

    pub fn is_rule_applied(&self, expr_id: ExprId, rule_id: RuleId) -> bool {
        self.state
            .read()
            .unwrap()
            .applied_rules
            .get(&expr_id)
            .map(|rules| rules.contains(&rule_id))
            .unwrap_or(false)
    }

    pub fn mark_rule_applied(&self, expr_id: ExprId, rule_id: RuleId) {
        self.state
            .write()
            .unwrap()
            .applied_rules
            .entry(expr_id)
            .or_insert_with(HashSet::new)
            .insert(rule_id);
    }

    pub fn add_expr_to_new_group(&self, expr: RelNodeRef<T>) -> (GroupId, ExprId) {
        self.state
            .write()
            .unwrap()
            .memo
            .add_new_group_expr(expr, None)
    }

    pub fn add_expr_to_group(&self, expr: RelNodeRef<T>, group_id: GroupId) -> (GroupId, ExprId) {
        self.state
            .write()
            .unwrap()
            .memo
            .add_new_group_expr(expr, Some(group_id))
    }

    pub fn get_expr_info(&self, expr: RelNodeRef<T>) -> (GroupId, ExprId) {
        self.state.read().unwrap().memo.get_expr_info(expr)
    }

    pub fn resolve_group_id(&self, root_rel: RelNodeRef<T>) -> GroupId {
        if let Some(group_id) = T::extract_group(&root_rel.typ) {
            return group_id;
        }
        let (group_id, _) = self.get_expr_info(root_rel);
        group_id
    }

    pub fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        self.state.read().unwrap().memo.get_group_id(expr_id)
    }

    pub fn get_group_info(&self, group_id: GroupId) -> GroupInfo {
        self.state.read().unwrap().memo.get_group_info(group_id)
    }

    pub fn update_group_info(&self, group_id: GroupId, new_info: GroupInfo) {
        self.state
            .write()
            .unwrap()
            .memo
            .update_group_info(group_id, new_info);
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
        self.state
            .read()
            .unwrap()
            .memo
            .get_group(group_id)
            .properties[idx]
            .downcast_ref::<P::Prop>()
            .unwrap()
            .clone()
    }

    pub fn cost(&self) -> &Arc<dyn CostModel<T>> {
        &self.cost
    }

    pub fn incr_transformation_count(&self) {
        self.transformation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn disable_rule(&mut self, rule_id: RuleId) {
        self.state.write().unwrap().disabled_rules.insert(rule_id);
    }

    pub fn enable_rule(&mut self, rule_id: RuleId) {
        self.state.write().unwrap().disabled_rules.remove(&rule_id);
    }

    /// Clear the optimizer state (including memo table)
    pub fn step_clear(&mut self) {
        *self.state.get_mut().unwrap() = CascadesOptimizerState {
            memo: Memo::new(self.property_builders.clone()),
            explored_groups: HashSet::new(),
            applied_rules: HashMap::new(),
            disabled_rules: HashSet::new(),
        };
    }

    /// Clear the winner so that the optimizer can continue to explore the group.
    pub fn step_clear_winner(&mut self) {
        self.state.write().unwrap().memo.clear_winner();
    }

    pub fn step_get_winner(
        &self,
        group_id: GroupId,
        meta: &mut Option<HashMap<usize, RelNodeMeta>>,
    ) -> Result<RelNodeRef<T>> {
        self.state
            .read()
            .unwrap()
            .memo
            .get_best_group_binding(group_id, meta)
    }

    pub fn step_optimize_group(&self, root_group_id: GroupId) -> Result<()> {
        {
            let mut tasks = self.tasks.lock().unwrap();
            if tasks.is_empty() {
                tasks.push_back(get_initial_task(root_group_id));
            }
        }

        // Run single-threaded search
        while let Some(task) = self.dequeue_task() {
            task.execute(self);
            // execute_task(self, task);
        }

        Ok(())
    }

    pub fn step_optimize_rel(&self, root_rel: RelNodeRef<T>) -> Result<GroupId> {
        let (root_group_id, _) = self.add_expr_to_new_group(root_rel);
        self.step_optimize_group(root_group_id)?;
        Ok(root_group_id)
    }

    fn optimize_inner(&self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        let root_group_id = self.step_optimize_rel(root_rel)?;
        self.state
            .read()
            .unwrap()
            .memo
            .get_best_group_binding(root_group_id, &mut None)
    }
}

impl<T: RelNodeTyp> Optimizer<T> for CascadesOptimizer<T> {
    fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        self.optimize_inner(root_rel)
    }

    fn get_property<P: PropertyBuilder<T>>(&self, root_rel: RelNodeRef<T>, idx: usize) -> P::Prop {
        self.get_property_by_group::<P>(self.resolve_group_id(root_rel), idx)
    }
}

/// Execute task asynchronously
/// TODO this is not tested or functional currently
// fn execute_task<T: RelNodeTyp>(optimizer: &CascadesOptimizer<T>, task: Box<dyn Task<T>>) {
//     tokio::spawn(async move {
//         task.execute(optimizer);
//     });
// }

pub fn rule_matches_expr<T: RelNodeTyp>(
    rule: &Arc<dyn Rule<T, CascadesOptimizer<T>>>,
    expr: &RelMemoNodeRef<T>,
) -> bool {
    let matcher = rule.matcher();
    let typ_to_match = &expr.typ;
    match matcher {
        RuleMatcher::MatchAndPickNode { typ, .. } => typ == typ_to_match,
        RuleMatcher::MatchNode { typ, .. } => typ == typ_to_match,
        _ => panic!("IR should have root node of match"), // TODO: what does this mean? replace text
    }
}
