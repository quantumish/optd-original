// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use itertools::Itertools;
use tracing::trace;

use super::optimizer::{ExprId, GroupId, PredId};
use crate::cost::{Cost, Statistics};
use crate::logical_property::{LogicalProperty, LogicalPropertyBuilderAny};
use crate::nodes::{ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeOrGroup};

pub type ArcMemoPlanNode<T> = Arc<MemoPlanNode<T>>;

/// The RelNode representation in the memo table. Store children as group IDs. Equivalent to MExpr
/// in Columbia/Cascades.
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
        for pred in &self.predicates {
            write!(f, " {}", pred)?;
        }
        write!(f, ")")
    }
}

#[derive(Clone)]
pub struct WinnerInfo {
    pub expr_id: ExprId,
    pub total_weighted_cost: f64,
    pub operation_weighted_cost: f64,
    pub total_cost: Cost,
    pub operation_cost: Cost,
    pub statistics: Arc<Statistics>,
}

#[derive(Clone)]
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

#[derive(Default, Clone)]
pub struct GroupInfo {
    pub winner: Winner,
}

pub struct Group {
    pub(crate) group_exprs: HashSet<ExprId>,
    pub(crate) info: GroupInfo,
    pub(crate) properties: Arc<[Box<dyn LogicalProperty>]>,
}

/// Trait for memo table implementations.
pub trait Memo<T: NodeType>: 'static + Send + Sync {
    /// Add an expression to the memo table. If the expression already exists, it will return the
    /// existing group id and expr id. Otherwise, a new group and expr will be created.
    fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId);

    /// Add a new expression to an existing gruop. If the expression is a group, it will merge the
    /// two groups. Otherwise, it will add the expression to the group. Returns the expr id if
    /// the expression is not a group.
    fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId>;

    /// Add a new predicate into the memo table.
    fn add_new_pred(&mut self, pred_node: ArcPredNode<T>) -> PredId;

    /// Get the group id of an expression.
    /// The group id is volatile, depending on whether the groups are merged.
    fn get_group_id(&self, expr_id: ExprId) -> GroupId;

    /// Get the memoized representation of a node.
    fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T>;

    /// Get all groups IDs in the memo table.
    fn get_all_group_ids(&self) -> Vec<GroupId>;

    /// Get a group by ID
    fn get_group(&self, group_id: GroupId) -> &Group;

    /// Get a predicate by ID
    fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T>;

    /// Update the group info.
    fn update_group_info(&mut self, group_id: GroupId, group_info: GroupInfo);

    /// Estimated plan space for the memo table, only useful when plan exploration budget is
    /// enabled. Returns number of expressions in the memo table.
    fn estimated_plan_space(&self) -> usize;

    // The below functions can be overwritten by the memo table implementation if there
    // are more efficient way to retrieve the information.

    /// Get all expressions in the group.
    fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        let group = self.get_group(group_id);
        let mut exprs = group.group_exprs.iter().copied().collect_vec();
        // Sort so that we can get a stable processing order for the expressions, therefore making regression test
        // yield a stable result across different platforms.
        exprs.sort();
        exprs
    }

    /// Get group info of a group.
    fn get_group_info(&self, group_id: GroupId) -> &GroupInfo {
        &self.get_group(group_id).info
    }

    /// Get the best group binding based on the cost
    fn get_best_group_binding(
        &self,
        group_id: GroupId,
        mut post_process: impl FnMut(ArcPlanNode<T>, GroupId, &WinnerInfo),
    ) -> Result<ArcPlanNode<T>> {
        let mut visited = HashSet::new();
        get_best_group_binding_inner(self, group_id, &mut post_process, &mut visited)
    }
}

fn get_best_group_binding_inner<M: Memo<T> + ?Sized, T: NodeType>(
    this: &M,
    group_id: GroupId,
    post_process: &mut impl FnMut(ArcPlanNode<T>, GroupId, &WinnerInfo),
    visited: &mut HashSet<GroupId>,
) -> Result<ArcPlanNode<T>> {
    if !visited.insert(group_id) {
        bail!("cycle detected in group {}", group_id);
    }
    visited.insert(group_id);
    let info: &GroupInfo = this.get_group_info(group_id);
    if let Winner::Full(info @ WinnerInfo { expr_id, .. }) = &info.winner {
        let expr = this.get_expr_memoed(*expr_id);
        let mut children = Vec::with_capacity(expr.children.len());
        for child in &expr.children {
            children.push(PlanNodeOrGroup::PlanNode(
                get_best_group_binding_inner(this, *child, post_process, visited)
                    .with_context(|| format!("when processing expr {}", expr_id))?,
            ));
        }
        let node = Arc::new(PlanNode {
            typ: expr.typ.clone(),
            children,
            predicates: expr.predicates.iter().map(|x| this.get_pred(*x)).collect(),
        });
        post_process(node.clone(), group_id, info);
        visited.remove(&group_id);
        return Ok(node);
    }
    bail!("no best group binding for group {}", group_id)
}

/// A naive, simple, and unoptimized memo table implementation.
pub struct NaiveMemo<T: NodeType> {
    // Source of truth.
    groups: HashMap<GroupId, Group>,
    expr_id_to_expr_node: HashMap<ExprId, ArcMemoPlanNode<T>>,

    // Predicate stuff.
    pred_id_to_pred_node: HashMap<PredId, ArcPredNode<T>>,
    pred_node_to_pred_id: HashMap<ArcPredNode<T>, PredId>,

    // Internal states.
    group_expr_counter: usize,
    property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,

    // Indexes.
    expr_fingerprint_to_ids: HashMap<u64, HashSet<ExprId>>,
    expr_id_to_group_id: HashMap<ExprId, GroupId>,

    // We update all group IDs in the memo table upon group merging, but
    // there might be edge cases that some tasks still hold the old group ID.
    // In this case, we need this mapping to redirect to the merged group ID.
    merged_group_mapping: HashMap<GroupId, GroupId>,
    dup_expr_mapping: HashMap<ExprId, ExprId>,
}

impl<T: NodeType> Memo<T> for NaiveMemo<T> {
    fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let (group_id, expr_id) = self
            .add_new_group_expr_inner(rel_node, None)
            .expect("should not trigger merge group");
        self.verify_integrity();
        (group_id, expr_id)
    }

    fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        match rel_node {
            PlanNodeOrGroup::Group(input_group) => {
                let input_group = self.reduce_group(input_group);
                let group_id = self.reduce_group(group_id);
                self.merge_group_inner(input_group, group_id);
                None
            }
            PlanNodeOrGroup::PlanNode(rel_node) => {
                let reduced_group_id = self.reduce_group(group_id);
                let (returned_group_id, expr_id) = self
                    .add_new_group_expr_inner(rel_node, Some(reduced_group_id))
                    .unwrap();
                assert_eq!(returned_group_id, reduced_group_id);
                self.verify_integrity();
                Some(expr_id)
            }
        }
    }

    fn add_new_pred(&mut self, pred_node: ArcPredNode<T>) -> PredId {
        let pred_id = self.next_pred_id();
        if let Some(id) = self.pred_node_to_pred_id.get(&pred_node) {
            return *id;
        }
        self.pred_node_to_pred_id.insert(pred_node.clone(), pred_id);
        self.pred_id_to_pred_node.insert(pred_id, pred_node);
        pred_id
    }

    fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T> {
        self.pred_id_to_pred_node[&pred_id].clone()
    }

    fn get_group_id(&self, mut expr_id: ExprId) -> GroupId {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        *self
            .expr_id_to_group_id
            .get(&expr_id)
            .expect("expr not found in group mapping")
    }

    fn get_expr_memoed(&self, mut expr_id: ExprId) -> ArcMemoPlanNode<T> {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        self.expr_id_to_expr_node
            .get(&expr_id)
            .expect("expr not found in expr mapping")
            .clone()
    }

    fn get_all_group_ids(&self) -> Vec<GroupId> {
        let mut ids = self.groups.keys().copied().collect_vec();
        ids.sort();
        ids
    }

    fn get_group(&self, group_id: GroupId) -> &Group {
        let group_id = self.reduce_group(group_id);
        self.groups.get(&group_id).as_ref().unwrap()
    }

    fn update_group_info(&mut self, group_id: GroupId, group_info: GroupInfo) {
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

    fn estimated_plan_space(&self) -> usize {
        self.expr_id_to_expr_node.len()
    }
}

impl<T: NodeType> NaiveMemo<T> {
    pub fn new(property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>) -> Self {
        Self {
            expr_id_to_group_id: HashMap::new(),
            expr_id_to_expr_node: HashMap::new(),
            expr_fingerprint_to_ids: HashMap::new(),
            pred_id_to_pred_node: HashMap::new(),
            pred_node_to_pred_id: HashMap::new(),
            groups: HashMap::new(),
            group_expr_counter: 0,
            merged_group_mapping: HashMap::new(),
            property_builders,
            dup_expr_mapping: HashMap::new(),
        }
    }

    /// Get the next group id. Group id and expr id shares the same counter, so as to make it easier
    /// to debug...
    fn next_group_id(&mut self) -> GroupId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        GroupId(id)
    }

    /// Get the next expr id. Group id and expr id shares the same counter, so as to make it easier
    /// to debug...
    fn next_expr_id(&mut self) -> ExprId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        ExprId(id)
    }

    /// Get the next pred id. Group id and expr id shares the same counter, so as to make it easier
    /// to debug...
    fn next_pred_id(&mut self) -> PredId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        PredId(id)
    }

    fn verify_integrity(&self) {}

    /// Get the fingerprint of a memoed plan node, all group id rewritten to the root
    fn fingerprint_of(&self, expr: &MemoPlanNode<T>) -> u64 {
        let mut rewritten_expr = expr.clone();
        for group in &mut rewritten_expr.children {
            *group = self.reduce_group(*group);
        }
        let mut hasher = DefaultHasher::new();
        rewritten_expr.hash(&mut hasher);
        hasher.finish()
    }

    /// Think of it as `get_root_group`
    fn reduce_group(&self, mut group_id: GroupId) -> GroupId {
        while let Some(parent_group_id) = self.merged_group_mapping.get(&group_id) {
            if group_id != *parent_group_id {
                group_id = *parent_group_id;
            } else {
                break;
            }
        }
        group_id
    }

    /// Look up the exact eq of an expr
    fn lookup_expr(&self, expr: &MemoPlanNode<T>) -> Option<ExprId> {
        let fingerprint = self.fingerprint_of(expr);
        if let Some(matches) = self.expr_fingerprint_to_ids.get(&fingerprint) {
            for potential_match_id in matches {
                let mut potential_match = self.expr_id_to_expr_node[potential_match_id]
                    .as_ref()
                    .clone();
                for child in &mut potential_match.children {
                    *child = self.reduce_group(*child);
                }
                if potential_match == *expr {
                    return Some(*potential_match_id);
                }
            }
        }
        None
    }

    fn get_group_set(&self, group_id: GroupId) -> Vec<GroupId> {
        let reduced_group_id = self.reduce_group(group_id);
        let mut set = Vec::new();
        for from in self.merged_group_mapping.keys() {
            if self.reduce_group(*from) == reduced_group_id {
                set.push(*from);
            }
        }
        set
    }

    fn merge_group_inner(&mut self, merge_into: GroupId, merge_from: GroupId) {
        trace!(event = "merge_group", merge_into = %merge_into, merge_from = %merge_from);
        let merge_into_root_id = self.reduce_group(merge_into);
        let merge_from_root_id = self.reduce_group(merge_from);
        if merge_into_root_id == merge_from_root_id {
            return;
        }
        let merge_from_group_set = self
            .get_group_set(merge_from_root_id)
            .into_iter()
            .collect::<HashSet<_>>();
        let expr_with_merge_from_child = self
            .expr_id_to_expr_node
            .iter()
            .filter(|(_, node)| {
                node.children
                    .iter()
                    .any(|child| merge_from_group_set.contains(child))
            })
            .map(|(id, expr)| (*id, expr.clone()))
            .collect_vec();
        self.merged_group_mapping
            .insert(merge_from_root_id, merge_into_root_id);
        for (expr_id, _) in expr_with_merge_from_child {
            // We already modified the group mapping, so `add_expr_to_fingerprint_index` will
            // generate a new fingerprint for the expr based on the updated group mapping.
            self.add_expr_to_fingerprint_index(expr_id);
        }
    }

    fn add_expr_to_fingerprint_index(&mut self, expr_id: ExprId) {
        let expr = self.expr_id_to_expr_node.get(&expr_id).unwrap();
        let fingerprint = self.fingerprint_of(expr);
        self.expr_fingerprint_to_ids
            .entry(fingerprint)
            .or_default()
            .insert(expr_id);
    }

    fn add_new_group_expr_inner(
        &mut self,
        rel_node: ArcPlanNode<T>,
        add_to_group_id: Option<GroupId>,
    ) -> anyhow::Result<(GroupId, ExprId)> {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| {
                match child {
                    // TODO: can I remove reduce?
                    PlanNodeOrGroup::Group(group) => self.reduce_group(*group),
                    PlanNodeOrGroup::PlanNode(child) => {
                        // No merge / modification to the memo should occur for the following
                        // operation
                        let (group, _) = self
                            .add_new_group_expr_inner(child.clone(), None)
                            .expect("should not trigger merge group");
                        self.reduce_group(group) // TODO: can I remove?
                    }
                }
            })
            .collect::<Vec<_>>();
        let memo_node = MemoPlanNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            predicates: rel_node
                .predicates
                .iter()
                .map(|x| self.add_new_pred(x.clone()))
                .collect(),
        };
        if let Some(expr_id) = self.lookup_expr(&memo_node) {
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
        self.add_expr_to_fingerprint_index(expr_id);
        self.append_expr_to_group(expr_id, group_id, memo_node);
        Ok((group_id, expr_id))
    }

    /// This is inefficient: usually the optimizer should have a MemoRef instead of passing the full
    /// rel node. Should be only used for debugging purpose.
    #[cfg(test)]
    pub(crate) fn get_expr_info(&self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| {
                let group_id = match child {
                    PlanNodeOrGroup::Group(group) => self.reduce_group(*group),
                    PlanNodeOrGroup::PlanNode(child) => self.get_expr_info(child.clone()).0,
                };
                self.reduce_group(group_id)
            })
            .collect::<Vec<_>>();
        let memo_node = MemoPlanNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            predicates: rel_node
                .predicates
                .iter()
                .map(|x| self.pred_node_to_pred_id[x])
                .collect(),
        };
        let Some(expr_id) = self.lookup_expr(&memo_node) else {
            unreachable!("not found {}", memo_node)
        };
        let group_id = self.expr_id_to_group_id[&expr_id];
        (group_id, expr_id)
    }

    fn infer_properties(&self, memo_node: MemoPlanNode<T>) -> Vec<Box<dyn LogicalProperty>> {
        let child_properties = memo_node
            .children
            .iter()
            .map(|child| self.groups[child].properties.clone())
            .collect_vec();
        let mut props = Vec::with_capacity(self.property_builders.len());
        for (id, builder) in self.property_builders.iter().enumerate() {
            let child_properties = child_properties
                .iter()
                .map(|x| x[id].as_ref())
                .collect::<Vec<_>>();
            let child_predicates = memo_node
                .predicates
                .iter()
                .map(|x| self.pred_id_to_pred_node[x].clone())
                .collect_vec();
            let prop = builder.derive_any(
                memo_node.typ.clone(),
                &child_predicates,
                child_properties.as_slice(),
            );
            props.push(prop);
        }
        props
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

    pub fn clear_winner(&mut self) {
        for group in self.groups.values_mut() {
            group.info.winner = Winner::Unknown;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        nodes::Value,
        tests::common::{
            expr, group, join, list, project, scan, MemoTestRelTyp, TestProp, TestPropertyBuilder,
        },
    };

    #[test]
    fn add_predicate() {
        let mut memo = NaiveMemo::<MemoTestRelTyp>::new(Arc::new([]));
        let pred_node = list(vec![expr(Value::Int32(233))]);
        let p1 = memo.add_new_pred(pred_node.clone());
        let p2 = memo.add_new_pred(pred_node.clone());
        assert_eq!(p1, p2);
    }

    #[test]
    fn group_merge_1() {
        let mut memo = NaiveMemo::new(Arc::new([]));
        let (group_id, _) =
            memo.add_new_expr(join(scan("t1"), scan("t2"), expr(Value::Bool(true))));
        memo.add_expr_to_group(
            join(scan("t2"), scan("t1"), expr(Value::Bool(true))).into(),
            group_id,
        );
        assert_eq!(memo.get_group(group_id).group_exprs.len(), 2);
    }

    #[test]
    fn group_merge_2() {
        let mut memo = NaiveMemo::new(Arc::new([]));
        let (group_id_1, _) = memo.add_new_expr(project(
            join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
            list(vec![expr(Value::Int64(1))]),
        ));
        let (group_id_2, _) = memo.add_new_expr(project(
            join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
            list(vec![expr(Value::Int64(1))]),
        ));
        assert_eq!(group_id_1, group_id_2);
    }

    #[test]
    fn group_merge_3() {
        let mut memo = NaiveMemo::new(Arc::new([]));
        let expr1 = project(scan("t1"), list(vec![expr(Value::Int64(1))]));
        let expr2 = project(scan("t1-alias"), list(vec![expr(Value::Int64(1))]));
        memo.add_new_expr(expr1.clone());
        memo.add_new_expr(expr2.clone());
        // merging two child groups causes parent to merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1"));
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr);
        let (group_1, _) = memo.get_expr_info(expr1);
        let (group_2, _) = memo.get_expr_info(expr2);
        assert_eq!(group_1, group_2);
    }

    #[test]
    fn group_merge_4() {
        let mut memo = NaiveMemo::new(Arc::new([]));
        let expr1 = project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let expr2 = project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        memo.add_new_expr(expr1.clone());
        memo.add_new_expr(expr2.clone());
        // merge two child groups, cascading merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1"));
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr);
        let (group_1, _) = memo.get_expr_info(expr1.clone());
        let (group_2, _) = memo.get_expr_info(expr2.clone());
        assert_eq!(group_1, group_2);
        let (group_1, _) = memo.get_expr_info(expr1.child_rel(0));
        let (group_2, _) = memo.get_expr_info(expr2.child_rel(0));
        assert_eq!(group_1, group_2);
    }

    #[test]
    fn group_merge_5() {
        let mut memo = NaiveMemo::new(Arc::new([]));
        let expr1 = project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let expr2 = project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let (_, expr1_id) = memo.add_new_expr(expr1.clone());
        let (_, expr2_id) = memo.add_new_expr(expr2.clone());

        // experimenting with group id in expr (i.e., when apply rules)
        let (scan_t1, _) = memo.get_expr_info(scan("t1"));
        let pred = list(vec![expr(Value::Int64(1))]);
        let proj_binding = project(group(scan_t1), pred);
        let middle_proj_2 = memo.get_expr_memoed(expr2_id).children[0];

        memo.add_expr_to_group(proj_binding.into(), middle_proj_2);

        // We don't rewrite the group id in the expr; instead, we update the fingerprint. So these
        // two exprs will never be the same.

        // assert_eq!(
        //     memo.get_expr_memoed(expr1_id),
        //     memo.get_expr_memoed(expr2_id)
        // ); // these two expressions are merged
        // assert_eq!(memo.get_expr_info(expr1), memo.get_expr_info(expr2));
    }

    #[test]
    fn derive_logical_property() {
        let mut memo = NaiveMemo::new(Arc::new([Box::new(TestPropertyBuilder)]));
        let (group_id, _) = memo.add_new_expr(join(
            scan("t1"),
            project(
                scan("t2"),
                list(vec![expr(Value::Int64(1)), expr(Value::Int64(2))]),
            ),
            expr(Value::Bool(true)),
        ));
        let group = memo.get_group(group_id);
        assert_eq!(group.properties.len(), 1);
        assert_eq!(
            group.properties[0]
                .as_any()
                .downcast_ref::<TestProp>()
                .unwrap()
                .0,
            vec!["scan_col", "1", "2"]
        );
    }
}
