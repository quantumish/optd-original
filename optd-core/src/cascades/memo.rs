// TODO: can i remove reduced group id entirely?
// TODO: Do we really need to have getters for all of these separate group fields instead of just getting everything? Maybe...
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use itertools::Itertools;
use std::any::Any;
use tracing::trace;

use crate::{
    cost::{Cost, Statistics},
    nodes::{
        ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeMeta, PlanNodeMetaMap,
        PlanNodeOrGroup, PredNode, Value,
    },
    property::PropertyBuilderAny,
};

use super::optimizer::{ExprId, GroupId};

// TODO: What is a RelMemoNodeRef supposed to mean?
// Can we call this a MemoExprRef, and MemoExpr instead?
pub type RelMemoNodeRef<T> = Arc<MemoPlanNode<T>>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum BindingType {
    Both,
    Logical,
    Physical,
}

/// Fully unmaterialized plan node for fast hashing in memo table.
/// Equivalent to an expression in Cascades.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemoPlanNode<T: NodeType> {
    pub typ: T,
    pub children: Vec<GroupId>,
    pub predicates: Vec<PredId>,
}

impl<T: NodeType> std::fmt::Display for MemoPlanNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        for child in &self.children {
            write!(f, " {}", child)?;
        }
        for child in &self.predicates {
            write!(f, " {}", child)?;
        }
        write!(f, ")")
    }
}

/// Fully unmaterialized predicate node for fast hashing in memo table.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemoPredNode<T: NodeType> {
    pub typ: T::PredType,
    pub children: Vec<PredId>,
    pub data: Option<Value>,
}

impl<T: NodeType> MemoPlanNode<T> {
    pub fn into_rel_node(self) -> PlanNode<T> {
        PlanNode {
            typ: self.typ,
            children: self
                .children
                .into_iter()
                .map(|x| PlanNodeOrGroup::Group(x))
                .collect(),
            predicates: vec![], // TODO: fill in the predicates
        }
    }
}

impl<T: NodeType> std::fmt::Display for MemoPredNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        for child in &self.children {
            write!(f, " {}", child)?;
        }
        write!(f, ")")
    }
}

#[derive(Debug, Clone)]
pub struct WinnerInfo {
    pub expr_id: ExprId,
    pub total_weighted_cost: f64,
    pub operation_weighted_cost: f64,
    pub total_cost: Cost,
    pub operation_cost: Cost,
    pub statistics: Arc<Statistics>,
}

#[derive(Debug, Clone)]
pub enum Winner {
    Unknown,
    Impossible,
    Full(WinnerInfo),
}

impl Winner {
    pub fn has_full_winner(&self) -> bool {
        matches!(self, Self::Full { .. })
    }

    pub fn has_decided(&self) -> bool {
        matches!(self, Self::Full { .. } | Self::Impossible)
    }

    pub fn as_full_winner(&self) -> Option<&WinnerInfo> {
        match self {
            Self::Full(info) => Some(info),
            _ => None,
        }
    }
}

impl Default for Winner {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Default, Debug, Clone)]
pub struct GroupInfo {
    pub winner: Winner,
}

pub(crate) struct Group {
    pub(crate) group_exprs: HashSet<ExprId>,
    pub(crate) info: GroupInfo,
    pub(crate) properties: Arc<[Box<dyn Any + Send + Sync + 'static>]>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct PredId(usize);

impl Display for PredId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "P{}", self.0)
    }
}

pub struct Memo<T: NodeType> {
    // Source of truth.
    groups: HashMap<GroupId, Group>,
    expr_id_to_expr_node: HashMap<ExprId, RelMemoNodeRef<T>>,

    // Predicate stuff (TODO: Improve this---this is a working prototype)
    pred_node_to_pred_id: HashMap<MemoPredNode<T>, PredId>, // TODO: Pred groups not implemented yet
    pred_id_to_pred_node: HashMap<PredId, MemoPredNode<T>>, // TODO: Pred groups not implemented yet

    // Internal states.
    group_expr_counter: usize,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,

    // Indexes.
    expr_node_to_expr_id: HashMap<MemoPlanNode<T>, ExprId>,
    expr_id_to_group_id: HashMap<ExprId, GroupId>,

    // We update all group IDs in the memo table upon group merging, but
    // there might be edge cases that some tasks still hold the old group ID.
    // In this case, we need this mapping to redirect to the merged group ID.
    merged_group_mapping: HashMap<GroupId, GroupId>,
    dup_expr_mapping: HashMap<ExprId, ExprId>,
}

impl<T: NodeType> Memo<T> {
    pub fn new(property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>) -> Self {
        Self {
            expr_id_to_group_id: HashMap::new(),
            expr_id_to_expr_node: HashMap::new(),
            expr_node_to_expr_id: HashMap::new(),
            groups: HashMap::new(),
            pred_node_to_pred_id: HashMap::new(),
            pred_id_to_pred_node: HashMap::new(),
            group_expr_counter: 0,
            merged_group_mapping: HashMap::new(),
            property_builders,
            dup_expr_mapping: HashMap::new(),
        }
    }

    /// Get the next group id. Group id and expr id shares the same counter, so as to make it easier to debug...
    fn next_group_id(&mut self) -> GroupId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        GroupId(id)
    }

    /// Get the next expr id. Group id and expr id shares the same counter, so as to make it easier to debug...
    fn next_expr_id(&mut self) -> ExprId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        ExprId(id)
    }

    fn verify_integrity(&self) {
        if cfg!(debug_assertions) {
            let num_of_exprs = self.expr_id_to_expr_node.len();
            assert_eq!(num_of_exprs, self.expr_node_to_expr_id.len());
            assert_eq!(num_of_exprs, self.expr_id_to_group_id.len());

            let mut valid_groups = HashSet::new();
            for to in self.merged_group_mapping.values() {
                assert_eq!(self.merged_group_mapping[to], *to);
                valid_groups.insert(*to);
            }
            assert_eq!(valid_groups.len(), self.groups.len());

            for (id, node) in self.expr_id_to_expr_node.iter() {
                assert_eq!(self.expr_node_to_expr_id[node], *id);
                for child in &node.children {
                    assert!(
                        valid_groups.contains(child),
                        "invalid group used in expression {}, where {} does not exist any more",
                        node,
                        child
                    );
                }
            }

            let mut cnt = 0;
            for (group_id, group) in &self.groups {
                assert!(valid_groups.contains(group_id));
                cnt += group.group_exprs.len();
                assert!(!group.group_exprs.is_empty());
                for expr in &group.group_exprs {
                    assert_eq!(self.expr_id_to_group_id[expr], *group_id);
                }
            }
            assert_eq!(cnt, num_of_exprs);
        }
    }

    #[allow(dead_code)]
    fn merge_group(&mut self, group_a: GroupId, group_b: GroupId) -> GroupId {
        use std::cmp::Ordering;
        let group_a = self.reduce_group(group_a);
        let group_b = self.reduce_group(group_b);
        let (merge_into, merge_from) = match group_a.0.cmp(&group_b.0) {
            Ordering::Less => (group_a, group_b),
            Ordering::Equal => return group_a,
            Ordering::Greater => (group_b, group_a),
        };
        self.merge_group_inner(merge_into, merge_from);
        self.verify_integrity();
        merge_into
    }

    /// Add an expression into the memo, returns the group id and the expr id.
    pub fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let (group_id, expr_id) = self
            .add_new_group_expr_inner(rel_node, None)
            .expect("should not trigger merge group");
        self.verify_integrity();
        (group_id, expr_id)
    }

    /// Add an expression into the memo, returns the expr id.
    pub fn add_expr_to_group(&mut self, rel_node: ArcPlanNode<T>, group_id: GroupId) -> ExprId {
        let reduced_group_id = self.reduce_group(group_id);
        let (returned_group_id, expr_id) = self
            .add_new_group_expr_inner(rel_node, Some(reduced_group_id))
            .unwrap();
        assert_eq!(returned_group_id, reduced_group_id);
        self.verify_integrity();
        expr_id
    }

    fn reduce_group(&self, group_id: GroupId) -> GroupId {
        self.merged_group_mapping[&group_id]
    }

    fn merge_group_inner(&mut self, merge_into: GroupId, merge_from: GroupId) {
        if merge_into == merge_from {
            return;
        }
        trace!(event = "merge_group", merge_into = %merge_into, merge_from = %merge_from);
        let group_merge_from = self.groups.remove(&merge_from).unwrap();
        let group_merge_into = self.groups.get_mut(&merge_into).unwrap();
        // TODO: update winner, cost and properties
        for from_expr in group_merge_from.group_exprs {
            let ret = self.expr_id_to_group_id.insert(from_expr, merge_into);
            assert!(ret.is_some());
            group_merge_into.group_exprs.insert(from_expr);
        }
        self.merged_group_mapping.insert(merge_from, merge_into);

        // Update all indexes and other data structures
        // 1. update merged group mapping -- could be optimized with union find
        for (_, mapped_to) in self.merged_group_mapping.iter_mut() {
            if *mapped_to == merge_from {
                *mapped_to = merge_into;
            }
        }

        let mut pending_recursive_merge = Vec::new();
        // 2. update all group expressions and indexes
        for (group_id, group) in self.groups.iter_mut() {
            let mut new_expr_list = HashSet::new();
            for expr_id in group.group_exprs.iter() {
                let expr = self.expr_id_to_expr_node[expr_id].clone();
                if expr.children.contains(&merge_from) {
                    // Create the new expr node
                    let old_expr = expr.as_ref().clone();
                    let mut new_expr = expr.as_ref().clone();
                    new_expr.children.iter_mut().for_each(|x| {
                        if *x == merge_from {
                            *x = merge_into;
                        }
                    });
                    // Update all existing entries and indexes
                    self.expr_id_to_expr_node
                        .insert(*expr_id, Arc::new(new_expr.clone()));
                    self.expr_node_to_expr_id.remove(&old_expr);
                    if let Some(dup_expr) = self.expr_node_to_expr_id.get(&new_expr) {
                        // If new_expr == some_other_old_expr in the memo table, unless they belong to the same group,
                        // we should merge the two groups. This should not happen. We should simply drop this expression.
                        let dup_group_id = self.expr_id_to_group_id[dup_expr];
                        if dup_group_id != *group_id {
                            pending_recursive_merge.push((dup_group_id, *group_id));
                        }
                        self.expr_id_to_expr_node.remove(expr_id);
                        self.expr_id_to_group_id.remove(expr_id);
                        self.dup_expr_mapping.insert(*expr_id, *dup_expr);
                        new_expr_list.insert(*dup_expr); // adding this temporarily -- should be removed once recursive merge finishes
                    } else {
                        self.expr_node_to_expr_id.insert(new_expr, *expr_id);
                        new_expr_list.insert(*expr_id);
                    }
                } else {
                    new_expr_list.insert(*expr_id);
                }
            }
            assert!(!new_expr_list.is_empty());
            group.group_exprs = new_expr_list;
        }
        for (merge_from, merge_into) in pending_recursive_merge {
            // We need to reduce because each merge would probably invalidate some groups in the last loop iteration.
            let merge_from = self.reduce_group(merge_from);
            let merge_into = self.reduce_group(merge_into);
            self.merge_group_inner(merge_into, merge_from);
        }
    }

    fn add_new_group_expr_inner(
        &mut self,
        plan_node: ArcPlanNode<T>,
        add_to_group_id: Option<GroupId>,
    ) -> anyhow::Result<(GroupId, ExprId)> {
        let children_group_ids = plan_node
            .children
            .iter()
            .map(|child| match child {
                PlanNodeOrGroup::Group(group_id) => *group_id,
                PlanNodeOrGroup::PlanNode(plan_node) => self.add_new_expr(plan_node.clone()).0,
            })
            .collect::<Vec<_>>();
        let predicates = plan_node
            .predicates
            .iter()
            .map(|pred| self.add_new_pred(pred.clone()))
            .collect::<Vec<_>>();
        let memo_node = MemoPlanNode {
            typ: plan_node.typ.clone(),
            children: children_group_ids,
            predicates: predicates,
        };
        if let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) {
            let group_id = self.expr_id_to_group_id[&expr_id];
            if let Some(add_to_group_id) = add_to_group_id {
                let add_to_group_id = self.reduce_group(add_to_group_id);
                self.merge_group_inner(add_to_group_id, group_id);
                return Ok((add_to_group_id, expr_id));
            }
            return Ok((group_id, expr_id));
        }
        let expr_id = self.next_expr_id();
        let group_id = if let Some(group_id) = add_to_group_id {
            group_id
        } else {
            self.next_group_id()
        };
        self.expr_id_to_expr_node
            .insert(expr_id, memo_node.clone().into());
        self.expr_id_to_group_id.insert(expr_id, group_id);
        self.expr_node_to_expr_id.insert(memo_node.clone(), expr_id);
        self.append_expr_to_group(expr_id, group_id, memo_node);
        Ok((group_id, expr_id))
    }

    /// This may also be inefficient for the same reason as get_expr_info.
    pub fn get_pred_expr_info(&self, pred_node: ArcPredNode<T>) -> PredId {
        let children_pred_ids = pred_node
            .children
            .iter()
            .map(|child| self.get_pred_expr_info(child.clone()))
            .collect::<Vec<_>>();

        let memo_node = MemoPredNode {
            typ: pred_node.typ.clone(),
            children: children_pred_ids,
            data: pred_node.data.clone(),
        };
        let Some(&pred_id) = self.pred_node_to_pred_id.get(&memo_node) else {
            unreachable!("not found {}", memo_node)
        };
        pred_id
    }
    /// This is inefficient: usually the optimizer should have a MemoRef instead of passing the full rel node.
    pub fn get_expr_info(&self, plan_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let children_group_ids = plan_node
            .children
            .iter()
            .map(|child| match child {
                PlanNodeOrGroup::Group(group_id) => *group_id,
                PlanNodeOrGroup::PlanNode(plan_node) => self.get_expr_info(plan_node.clone()).0,
            })
            .collect::<Vec<_>>();

        let pred_group_ids = plan_node
            .predicates
            .iter()
            .map(|pred| self.get_pred_expr_info(pred.clone()))
            .collect::<Vec<_>>();
        let memo_node = MemoPlanNode {
            typ: plan_node.typ.clone(),
            children: children_group_ids,
            predicates: pred_group_ids,
        };
        let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) else {
            unreachable!("not found {}", memo_node)
        };
        let group_id = self.expr_id_to_group_id[&expr_id];
        (group_id, expr_id)
    }

    fn infer_properties(
        &self,
        memo_node: MemoPlanNode<T>,
    ) -> Vec<Box<dyn Any + 'static + Send + Sync>> {
        let child_properties = memo_node
            .children
            .iter()
            .map(|child| self.groups[child].properties.clone())
            .collect_vec();
        let mut props = Vec::with_capacity(self.property_builders.len());
        for (id, builder) in self.property_builders.iter().enumerate() {
            let child_properties = child_properties
                .iter()
                .map(|x| x[id].as_ref() as &dyn std::any::Any)
                .collect::<Vec<_>>();
            let materialized_predicates = memo_node
                .predicates
                .iter()
                .map(|pred_id| self.get_pred_from_pred_id(*pred_id))
                .collect::<Vec<_>>();
            let prop = builder.derive_any(
                memo_node.typ.clone(),
                &materialized_predicates,
                child_properties.as_slice(),
            );
            props.push(prop);
        }
        props
    }

    fn add_new_pred(&mut self, pred_node: ArcPredNode<T>) -> PredId {
        let children_pred_ids = pred_node
            .children
            .iter()
            .map(|child| self.get_pred_expr_info(pred_node.clone()))
            .collect::<Vec<_>>();

        let memo_node = MemoPredNode {
            typ: pred_node.typ.clone(),
            children: children_pred_ids,
            data: pred_node.data.clone(),
        };
        if let Some(&pred_id) = self.pred_node_to_pred_id.get(&memo_node) {
            return pred_id;
        }
        let pred_id = PredId(self.pred_node_to_pred_id.len());
        self.pred_node_to_pred_id.insert(memo_node.clone(), pred_id);
        self.pred_id_to_pred_node.insert(pred_id, memo_node);
        pred_id
    }

    /// If group_id exists, it adds expr_id to the existing group
    /// Otherwise, it creates a new group of that group_id and insert expr_id into the new group
    fn append_expr_to_group(
        &mut self,
        expr_id: ExprId,
        group_id: GroupId,
        memo_node: MemoPlanNode<T>,
    ) {
        trace!(event = "add_expr_to_group", group_id = %group_id, expr_id = %expr_id, memo_node = %memo_node);
        if let Entry::Occupied(mut entry) = self.groups.entry(group_id) {
            let group = entry.get_mut();
            group.group_exprs.insert(expr_id);
            return;
        }
        // Create group and infer properties (only upon initializing a group).
        let mut group = Group {
            group_exprs: HashSet::new(),
            info: GroupInfo::default(),
            properties: self.infer_properties(memo_node).into(),
        };
        group.group_exprs.insert(expr_id);
        self.groups.insert(group_id, group);
        self.merged_group_mapping.insert(group_id, group_id);
    }

    /// Get the group id of an expression.
    /// The group id is volatile, depending on whether the groups are merged.
    pub fn get_group_id(&self, mut expr_id: ExprId) -> GroupId {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        *self
            .expr_id_to_group_id
            .get(&expr_id)
            .expect("expr not found in group mapping")
    }

    /// Get the memoized representation of a node, only for debugging purpose
    pub fn get_expr_memoed(&self, mut expr_id: ExprId) -> RelMemoNodeRef<T> {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        self.expr_id_to_expr_node
            .get(&expr_id)
            .expect("expr not found in expr mapping")
            .clone()
    }

    pub fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        let group_id = self.reduce_group(group_id);
        let group = self.groups.get(&group_id).expect("group not found");
        let mut exprs = group.group_exprs.iter().copied().collect_vec();
        exprs.sort();
        exprs
    }

    pub(crate) fn get_all_group_ids(&self) -> Vec<GroupId> {
        let mut ids = self.groups.keys().copied().collect_vec();
        ids.sort();
        ids
    }

    pub(crate) fn get_group_info(&self, group_id: GroupId) -> GroupInfo {
        let group_id = self.reduce_group(group_id);
        self.groups.get(&group_id).as_ref().unwrap().info.clone()
    }

    pub(crate) fn get_group(&self, group_id: GroupId) -> &Group {
        let group_id = self.reduce_group(group_id);
        self.groups.get(&group_id).as_ref().unwrap()
    }

    // TODO: I think the idea of a group info and the group cost/winner info should
    // be separated
    pub fn update_group_info(&mut self, group_id: GroupId, group_info: GroupInfo) {
        if let Winner::Full(WinnerInfo {
            total_weighted_cost,
            expr_id,
            ..
        }) = &group_info.winner
        {
            assert!(
                *total_weighted_cost != 0.0,
                "{}",
                self.expr_id_to_expr_node[expr_id]
            );
        }
        let grp = self.groups.get_mut(&group_id);
        grp.unwrap().info = group_info;
    }

    pub fn get_best_group_binding(
        &self,
        group_id: GroupId,
        post_process: &mut impl FnMut(Arc<RelNode<T>>, GroupId, &WinnerInfo),
    ) -> Result<ArcPlanNode<T>> {
        if let Winner::Full(info @ WinnerInfo { expr_id, .. }) = info.winner {
            let expr = self.expr_id_to_expr_node[&expr_id].clone();
            let mut children = Vec::with_capacity(expr.children.len());
            for child in &expr.children {
                children.push(PlanNodeOrGroup::PlanNode(
                    self.get_best_group_binding_inner(*child, post_process)
                        .with_context(|| format!("when processing expr {}", expr_id))?,
                ));
            }
            let mut predicates = Vec::with_capacity(expr.predicates.len());
            for pred in &expr.predicates {
                predicates.push(self.get_pred_from_pred_id(*pred));
            }
            let node = Arc::new(PlanNode {
                typ: expr.typ.clone(),
                children,
                predicates,
            });
            post_process(node.clone(), group_id, &info);
            return Ok(node);
        }
        bail!("no best group binding for group {}", group_id)
    }

    // todo: would be implemented differently with predicate groups
    pub fn get_pred_from_pred_id(&self, pred_id: PredId) -> ArcPredNode<T> {
        let pred_node = self.pred_id_to_pred_node[&pred_id].clone();
        // recursively materialize
        let children = pred_node
            .children
            .iter()
            .map(|child| self.get_pred_from_pred_id(*child))
            .collect_vec();
        Arc::new(PredNode {
            typ: pred_node.typ,
            children,
            data: pred_node.data.clone(),
        })
    }

    pub fn clear_winner(&mut self) {
        for group in self.groups.values_mut() {
            group.info.winner = Winner::Unknown;
        }
    }

    /// Return number of expressions in the memo table.
    pub fn compute_plan_space(&self) -> usize {
        self.expr_id_to_expr_node.len()
    }
}

// TODO: Fix tests with predicates
// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[derive(Debug, Clone, PartialEq, Eq, Hash)]
//     enum MemoTestRelTyp {
//         Group(GroupId),
//         List,
//         Join,
//         Project,
//         Scan,
//         Expr,
//     }

//     impl std::fmt::Display for MemoTestRelTyp {
//         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//             match self {
//                 Self::Group(x) => write!(f, "{}", x),
//                 other => write!(f, "{:?}", other),
//             }
//         }
//     }

//     impl NodeType for MemoTestRelTyp {
//         fn is_logical(&self) -> bool {
//             matches!(self, Self::Project | Self::Scan | Self::Join)
//         }
//     }

//     type MemoTestRelNode = RelNode<MemoTestRelTyp>;
//     type MemoTestRelNodeRef = RelNodeRef<MemoTestRelTyp>;

//     fn join(
//         left: impl Into<MemoTestRelNodeRef>,
//         right: impl Into<MemoTestRelNodeRef>,
//         cond: impl Into<MemoTestRelNodeRef>,
//     ) -> MemoTestRelNode {
//         RelNode {
//             typ: MemoTestRelTyp::Join,
//             children: vec![left.into(), right.into(), cond.into()],
//             data: None,
//         }
//     }

//     fn scan(table: &str) -> MemoTestRelNode {
//         RelNode {
//             typ: MemoTestRelTyp::Scan,
//             children: vec![],
//             data: Some(Value::String(table.to_string().into())),
//         }
//     }

//     fn project(
//         input: impl Into<MemoTestRelNodeRef>,
//         expr_list: impl Into<MemoTestRelNodeRef>,
//     ) -> MemoTestRelNode {
//         RelNode {
//             typ: MemoTestRelTyp::Project,
//             children: vec![input.into(), expr_list.into()],
//             data: None,
//         }
//     }

//     fn list(items: Vec<impl Into<MemoTestRelNodeRef>>) -> MemoTestRelNode {
//         RelNode {
//             typ: MemoTestRelTyp::List,
//             children: items.into_iter().map(|x| x.into()).collect(),
//             data: None,
//         }
//     }

//     fn expr(data: Value) -> MemoTestRelNode {
//         RelNode {
//             typ: MemoTestRelTyp::Expr,
//             children: vec![],
//             data: Some(data),
//         }
//     }

//     fn group(group_id: GroupId) -> MemoTestRelNode {
//         RelNode {
//             typ: MemoTestRelTyp::Group(group_id),
//             children: vec![],
//             data: None,
//         }
//     }

//     #[test]
//     fn group_merge_1() {
//         let mut memo = Memo::new(Arc::new([]));
//         let (group_id, _) =
//             memo.add_new_expr(join(scan("t1"), scan("t2"), expr(Value::Bool(true))).into());
//         memo.add_expr_to_group(
//             join(scan("t2"), scan("t1"), expr(Value::Bool(true))).into(),
//             group_id,
//         );
//         assert_eq!(memo.get_group(group_id).group_exprs.len(), 2);
//     }

//     #[test]
//     fn group_merge_2() {
//         let mut memo = Memo::new(Arc::new([]));
//         let (group_id_1, _) = memo.add_new_expr(
//             project(
//                 join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
//                 list(vec![expr(Value::Int64(1))]),
//             )
//             .into(),
//         );
//         let (group_id_2, _) = memo.add_new_expr(
//             project(
//                 join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
//                 list(vec![expr(Value::Int64(1))]),
//             )
//             .into(),
//         );
//         assert_eq!(group_id_1, group_id_2);
//     }

//     #[test]
//     fn group_merge_3() {
//         let mut memo = Memo::new(Arc::new([]));
//         let expr1 = Arc::new(project(scan("t1"), list(vec![expr(Value::Int64(1))])));
//         let expr2 = Arc::new(project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])));
//         memo.add_new_expr(expr1.clone());
//         memo.add_new_expr(expr2.clone());
//         // merging two child groups causes parent to merge
//         let (group_id_expr, _) = memo.get_expr_info(scan("t1").into());
//         memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr);
//         let (group_1, _) = memo.get_expr_info(expr1);
//         let (group_2, _) = memo.get_expr_info(expr2);
//         assert_eq!(group_1, group_2);
//     }

//     #[test]
//     fn group_merge_4() {
//         let mut memo = Memo::new(Arc::new([]));
//         let expr1 = Arc::new(project(
//             project(scan("t1"), list(vec![expr(Value::Int64(1))])),
//             list(vec![expr(Value::Int64(2))]),
//         ));
//         let expr2 = Arc::new(project(
//             project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
//             list(vec![expr(Value::Int64(2))]),
//         ));
//         memo.add_new_expr(expr1.clone());
//         memo.add_new_expr(expr2.clone());
//         // merge two child groups, cascading merge
//         let (group_id_expr, _) = memo.get_expr_info(scan("t1").into());
//         memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr);
//         let (group_1, _) = memo.get_expr_info(expr1.clone());
//         let (group_2, _) = memo.get_expr_info(expr2.clone());
//         assert_eq!(group_1, group_2);
//         let (group_1, _) = memo.get_expr_info(expr1.child(0));
//         let (group_2, _) = memo.get_expr_info(expr2.child(0));
//         assert_eq!(group_1, group_2);
//     }

//     #[test]
//     fn group_merge_5() {
//         let mut memo = Memo::new(Arc::new([]));
//         let expr1 = Arc::new(project(
//             project(scan("t1"), list(vec![expr(Value::Int64(1))])),
//             list(vec![expr(Value::Int64(2))]),
//         ));
//         let expr2 = Arc::new(project(
//             project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
//             list(vec![expr(Value::Int64(2))]),
//         ));
//         let (_, expr1_id) = memo.add_new_expr(expr1.clone());
//         let (_, expr2_id) = memo.add_new_expr(expr2.clone());

//         // experimenting with group id in expr (i.e., when apply rules)
//         let (scan_t1, _) = memo.get_expr_info(scan("t1").into());
//         let (expr_middle_proj, _) = memo.get_expr_info(list(vec![expr(Value::Int64(1))]).into());
//         let proj_binding = project(group(scan_t1), group(expr_middle_proj));
//         let middle_proj_2 = memo.get_expr_memoed(expr2_id).children[0];

//         memo.add_expr_to_group(proj_binding.into(), middle_proj_2);

//         assert_eq!(
//             memo.get_expr_memoed(expr1_id),
//             memo.get_expr_memoed(expr2_id)
//         ); // these two expressions are merged
//         assert_eq!(memo.get_expr_info(expr1), memo.get_expr_info(expr2));
//     }
// }
