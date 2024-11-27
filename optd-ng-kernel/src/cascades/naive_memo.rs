// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use tracing::trace;

use super::memo::{ArcMemoPlanNode, Group, Memo, MemoPlanNode, Winner};
use super::optimizer::{ExprId, GroupId, PredId};
use crate::cascades::memo::GroupInfo;
use crate::nodes::{ArcPlanNode, ArcPredNode, NodeType, PlanNodeOrGroup};

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

    // Indexes.
    expr_node_to_expr_id: HashMap<MemoPlanNode<T>, ExprId>,
    expr_id_to_group_id: HashMap<ExprId, GroupId>,

    // We update all group IDs in the memo table upon group merging, but
    // there might be edge cases that some tasks still hold the old group ID.
    // In this case, we need this mapping to redirect to the merged group ID.
    merged_group_mapping: HashMap<GroupId, GroupId>,
    dup_expr_mapping: HashMap<ExprId, ExprId>,
}

#[async_trait]
impl<T: NodeType> Memo<T> for NaiveMemo<T> {
    async fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        self.add_new_expr_inner(rel_node)
    }

    async fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        self.add_expr_to_group_inner(rel_node, group_id)
    }

    async fn add_new_pred(&mut self, pred_node: ArcPredNode<T>) -> PredId {
        self.add_new_pred_inner(pred_node)
    }

    async fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T> {
        self.get_pred_inner(pred_id)
    }

    async fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        self.get_group_id_inner(expr_id)
    }

    async fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T> {
        self.get_expr_memoed_inner(expr_id)
    }

    async fn get_all_group_ids(&self) -> Vec<GroupId> {
        self.get_all_group_ids_inner()
    }

    async fn get_group(&self, group_id: GroupId) -> &Group {
        self.get_group_inner(group_id)
    }

    async fn estimated_plan_space(&self) -> usize {
        self.expr_id_to_expr_node.len()
    }
}

impl<T: NodeType> NaiveMemo<T> {
    pub fn new() -> Self {
        Self {
            expr_id_to_group_id: HashMap::new(),
            expr_id_to_expr_node: HashMap::new(),
            expr_node_to_expr_id: HashMap::new(),
            pred_id_to_pred_node: HashMap::new(),
            pred_node_to_pred_id: HashMap::new(),
            groups: HashMap::new(),
            group_expr_counter: 0,
            merged_group_mapping: HashMap::new(),
            dup_expr_mapping: HashMap::new(),
        }
    }

    fn add_new_expr_inner(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let (group_id, expr_id) = self
            .add_new_group_expr_inner(rel_node, None)
            .expect("should not trigger merge group");
        self.verify_integrity();
        (group_id, expr_id)
    }

    fn add_expr_to_group_inner(
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

    fn add_new_pred_inner(&mut self, pred_node: ArcPredNode<T>) -> PredId {
        let pred_id = self.next_pred_id();
        if let Some(id) = self.pred_node_to_pred_id.get(&pred_node) {
            return *id;
        }
        self.pred_node_to_pred_id.insert(pred_node.clone(), pred_id);
        self.pred_id_to_pred_node.insert(pred_id, pred_node);
        pred_id
    }

    fn get_pred_inner(&self, pred_id: PredId) -> ArcPredNode<T> {
        self.pred_id_to_pred_node[&pred_id].clone()
    }

    fn get_group_id_inner(&self, mut expr_id: ExprId) -> GroupId {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        *self
            .expr_id_to_group_id
            .get(&expr_id)
            .expect("expr not found in group mapping")
    }

    fn get_expr_memoed_inner(&self, mut expr_id: ExprId) -> ArcMemoPlanNode<T> {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        self.expr_id_to_expr_node
            .get(&expr_id)
            .expect("expr not found in expr mapping")
            .clone()
    }

    fn get_all_group_ids_inner(&self) -> Vec<GroupId> {
        let mut ids = self.groups.keys().copied().collect_vec();
        ids.sort();
        ids
    }

    fn get_group_inner(&self, group_id: GroupId) -> &Group {
        let group_id = self.reduce_group(group_id);
        self.groups.get(&group_id).as_ref().unwrap()
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
                        // If new_expr == some_other_old_expr in the memo table, unless they belong
                        // to the same group, we should merge the two
                        // groups. This should not happen. We should simply drop this expression.
                        let dup_group_id = self.expr_id_to_group_id[dup_expr];
                        if dup_group_id != *group_id {
                            pending_recursive_merge.push((dup_group_id, *group_id));
                        }
                        self.expr_id_to_expr_node.remove(expr_id);
                        self.expr_id_to_group_id.remove(expr_id);
                        self.dup_expr_mapping.insert(*expr_id, *dup_expr);
                        new_expr_list.insert(*dup_expr); // adding this temporarily -- should be
                                                         // removed once recursive merge finishes
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
            // We need to reduce because each merge would probably invalidate some groups in the
            // last loop iteration.
            let merge_from = self.reduce_group(merge_from);
            let merge_into = self.reduce_group(merge_into);
            self.merge_group_inner(merge_into, merge_from);
        }
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
                .map(|x| self.add_new_pred_inner(x.clone()))
                .collect(),
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

    /// This is inefficient: usually the optimizer should have a MemoRef instead of passing the full
    /// rel node. Should be only used for debugging purpose.
    #[cfg(test)]
    pub(crate) fn get_expr_info(&self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| match child {
                PlanNodeOrGroup::Group(group) => *group,
                PlanNodeOrGroup::PlanNode(child) => self.get_expr_info(child.clone()).0,
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
        let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) else {
            unreachable!("not found {}", memo_node)
        };
        let group_id = self.expr_id_to_group_id[&expr_id];
        (group_id, expr_id)
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
        tests::common::{expr, group, join, list, project, scan, MemoTestRelTyp},
    };

    #[tokio::test]
    async fn add_predicate() {
        let mut memo = NaiveMemo::<MemoTestRelTyp>::new();
        let pred_node = list(vec![expr(Value::Int32(233))]);
        let p1 = memo.add_new_pred(pred_node.clone()).await;
        let p2 = memo.add_new_pred(pred_node.clone()).await;
        assert_eq!(p1, p2);
    }

    #[tokio::test]
    async fn group_merge_1() {
        let mut memo = NaiveMemo::new();
        let (group_id, _) = memo
            .add_new_expr(join(scan("t1"), scan("t2"), expr(Value::Bool(true))))
            .await;
        memo.add_expr_to_group(
            join(scan("t2"), scan("t1"), expr(Value::Bool(true))).into(),
            group_id,
        )
        .await;
        assert_eq!(memo.get_group(group_id).await.group_exprs.len(), 2);
    }

    #[tokio::test]
    async fn group_merge_2() {
        let mut memo = NaiveMemo::new();
        let (group_id_1, _) = memo
            .add_new_expr(project(
                join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
                list(vec![expr(Value::Int64(1))]),
            ))
            .await;
        let (group_id_2, _) = memo
            .add_new_expr(project(
                join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
                list(vec![expr(Value::Int64(1))]),
            ))
            .await;
        assert_eq!(group_id_1, group_id_2);
    }

    #[tokio::test]
    async fn group_merge_3() {
        let mut memo = NaiveMemo::new();
        let expr1 = project(scan("t1"), list(vec![expr(Value::Int64(1))]));
        let expr2 = project(scan("t1-alias"), list(vec![expr(Value::Int64(1))]));
        memo.add_new_expr(expr1.clone()).await;
        memo.add_new_expr(expr2.clone()).await;
        // merging two child groups causes parent to merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1"));
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr)
            .await;
        let (group_1, _) = memo.get_expr_info(expr1);
        let (group_2, _) = memo.get_expr_info(expr2);
        assert_eq!(group_1, group_2);
    }

    #[tokio::test]
    async fn group_merge_4() {
        let mut memo = NaiveMemo::new();
        let expr1 = project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let expr2 = project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        memo.add_new_expr(expr1.clone()).await;
        memo.add_new_expr(expr2.clone()).await;
        // merge two child groups, cascading merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1"));
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr)
            .await;
        let (group_1, _) = memo.get_expr_info(expr1.clone());
        let (group_2, _) = memo.get_expr_info(expr2.clone());
        assert_eq!(group_1, group_2);
        let (group_1, _) = memo.get_expr_info(expr1.child_rel(0));
        let (group_2, _) = memo.get_expr_info(expr2.child_rel(0));
        assert_eq!(group_1, group_2);
    }

    #[tokio::test]
    async fn group_merge_5() {
        let mut memo = NaiveMemo::new();
        let expr1 = project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let expr2 = project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let (_, expr1_id) = memo.add_new_expr(expr1.clone()).await;
        let (_, expr2_id) = memo.add_new_expr(expr2.clone()).await;

        // experimenting with group id in expr (i.e., when apply rules)
        let (scan_t1, _) = memo.get_expr_info(scan("t1"));
        let pred = list(vec![expr(Value::Int64(1))]);
        let proj_binding = project(group(scan_t1), pred);
        let middle_proj_2 = memo.get_expr_memoed(expr2_id).await.children[0];

        memo.add_expr_to_group(proj_binding.into(), middle_proj_2)
            .await;

        assert_eq!(
            memo.get_expr_memoed(expr1_id).await,
            memo.get_expr_memoed(expr2_id).await
        ); // these two expressions are merged
        assert_eq!(memo.get_expr_info(expr1), memo.get_expr_info(expr2));
    }
}
