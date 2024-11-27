// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;

use super::optimizer::{ExprId, GroupId, PredId};
use crate::nodes::{ArcPlanNode, ArcPredNode, NodeType, PlanNodeOrGroup};
use async_trait::async_trait;

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
pub struct WinnerInfo {/* unimplemented */}

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
}

/// Trait for memo table implementations. TODO: use GAT in the future.
#[async_trait]
pub trait Memo<T: NodeType>: 'static + Send + Sync {
    /// Add an expression to the memo table. If the expression already exists, it will return the
    /// existing group id and expr id. Otherwise, a new group and expr will be created.
    async fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId);

    /// Add a new expression to an existing gruop. If the expression is a group, it will merge the
    /// two groups. Otherwise, it will add the expression to the group. Returns the expr id if
    /// the expression is not a group.
    async fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId>;

    /// Add a new predicate into the memo table.
    async fn add_new_pred(&mut self, pred_node: ArcPredNode<T>) -> PredId;

    /// Get the group id of an expression.
    /// The group id is volatile, depending on whether the groups are merged.
    async fn get_group_id(&self, expr_id: ExprId) -> GroupId;

    /// Get the memoized representation of a node.
    async fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T>;

    /// Get all groups IDs in the memo table.
    async fn get_all_group_ids(&self) -> Vec<GroupId>;

    /// Get a group by ID
    async fn get_group(&self, group_id: GroupId) -> &Group;

    /// Get a predicate by ID
    async fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T>;

    /// Estimated plan space for the memo table, only useful when plan exploration budget is
    /// enabled. Returns number of expressions in the memo table.
    async fn estimated_plan_space(&self) -> usize;

    // The below functions can be overwritten by the memo table implementation if there
    // are more efficient way to retrieve the information.

    /// Get all expressions in the group.
    async fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        let group = self.get_group(group_id).await;
        let mut exprs = group.group_exprs.iter().copied().collect_vec();
        // Sort so that we can get a stable processing order for the expressions, therefore making regression test
        // yield a stable result across different platforms.
        exprs.sort();
        exprs
    }

    /// Get group info of a group.
    async fn get_group_info(&self, group_id: GroupId) -> &GroupInfo {
        &self.get_group(group_id).await.info
    }
}
