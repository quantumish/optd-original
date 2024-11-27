// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    cascades::optimizer::GroupId,
    nodes::{
        ArcPlanNode, ArcPredNode, NodeType, PersistentNodeType, PlanNode, PlanNodeOrGroup,
        PredNode, Value,
    },
};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum MemoTestRelTyp {
    Join,
    Project,
    Scan,
    Sort,
    Filter,
    Agg,
    PhysicalNestedLoopJoin,
    PhysicalProject,
    PhysicalFilter,
    PhysicalScan,
    PhysicalSort,
    PhysicalPartition,
    PhysicalStreamingAgg,
    PhysicalHashAgg,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum MemoTestPredTyp {
    List,
    Expr,
    TableName,
    ColumnRef,
}

impl std::fmt::Display for MemoTestRelTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Display for MemoTestPredTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl NodeType for MemoTestRelTyp {
    type PredType = MemoTestPredTyp;

    fn is_logical(&self) -> bool {
        matches!(
            self,
            Self::Project | Self::Scan | Self::Join | Self::Sort | Self::Filter
        )
    }
}

// TODO: move this into nodes.rs?
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentPredNode<T: PersistentNodeType> {
    /// A generic predicate node type
    pub typ: T::PredType,
    /// Child predicate nodes, always materialized
    pub children: Vec<PersistentPredNode<T>>,
    /// Data associated with the predicate, if any
    pub data: Option<Value>,
}

impl<T: PersistentNodeType> From<PersistentPredNode<T>> for PredNode<T> {
    fn from(node: PersistentPredNode<T>) -> Self {
        PredNode {
            typ: node.typ,
            children: node
                .children
                .into_iter()
                .map(|x| Arc::new(x.into()))
                .collect(),
            data: node.data,
        }
    }
}

impl<T: PersistentNodeType> From<PredNode<T>> for PersistentPredNode<T> {
    fn from(node: PredNode<T>) -> Self {
        PersistentPredNode {
            typ: node.typ,
            children: node
                .children
                .into_iter()
                .map(|x| x.as_ref().clone().into())
                .collect(),
            data: node.data,
        }
    }
}

impl PersistentNodeType for MemoTestRelTyp {
    fn serialize_pred(pred: &ArcPredNode<Self>) -> serde_json::Value {
        let node: PersistentPredNode<MemoTestRelTyp> = pred.as_ref().clone().into();
        serde_json::to_value(node).unwrap()
    }

    fn deserialize_pred(data: serde_json::Value) -> ArcPredNode<Self> {
        let node: PersistentPredNode<MemoTestRelTyp> = serde_json::from_value(data).unwrap();
        Arc::new(node.into())
    }

    fn serialize_plan_tag(tag: Self) -> serde_json::Value {
        serde_json::to_value(tag).unwrap()
    }

    fn deserialize_plan_tag(data: serde_json::Value) -> Self {
        serde_json::from_value(data).unwrap()
    }
}

pub(crate) fn join(
    left: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    right: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    cond: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::Join,
        children: vec![left.into(), right.into()],
        predicates: vec![cond],
    })
}

#[allow(dead_code)]
pub(crate) fn agg(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    group_bys: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::Agg,
        children: vec![input.into()],
        predicates: vec![group_bys],
    })
}

pub(crate) fn scan(table: &str) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::Scan,
        children: vec![],
        predicates: vec![table_name(table)],
    })
}

pub(crate) fn table_name(table: &str) -> ArcPredNode<MemoTestRelTyp> {
    Arc::new(PredNode {
        typ: MemoTestPredTyp::TableName,
        children: vec![],
        data: Some(Value::String(table.to_string().into())),
    })
}

pub(crate) fn project(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    expr_list: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::Project,
        children: vec![input.into()],
        predicates: vec![expr_list],
    })
}

pub(crate) fn physical_nested_loop_join(
    left: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    right: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    cond: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalNestedLoopJoin,
        children: vec![left.into(), right.into()],
        predicates: vec![cond],
    })
}

#[allow(dead_code)]
pub(crate) fn physical_project(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    expr_list: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalProject,
        children: vec![input.into()],
        predicates: vec![expr_list],
    })
}

pub(crate) fn physical_filter(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    cond: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalFilter,
        children: vec![input.into()],
        predicates: vec![cond],
    })
}

pub(crate) fn physical_scan(table: &str) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalScan,
        children: vec![],
        predicates: vec![table_name(table)],
    })
}

pub(crate) fn physical_sort(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    sort_expr: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalSort,
        children: vec![input.into()],
        predicates: vec![sort_expr],
    })
}

#[allow(dead_code)]
pub(crate) fn physical_partition(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    partition_expr: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalPartition,
        children: vec![input.into()],
        predicates: vec![partition_expr],
    })
}

pub(crate) fn physical_streaming_agg(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    group_bys: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalStreamingAgg,
        children: vec![input.into()],
        predicates: vec![group_bys],
    })
}

pub(crate) fn physical_hash_agg(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    group_bys: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalHashAgg,
        children: vec![input.into()],
        predicates: vec![group_bys],
    })
}

pub(crate) fn list(items: Vec<ArcPredNode<MemoTestRelTyp>>) -> ArcPredNode<MemoTestRelTyp> {
    Arc::new(PredNode {
        typ: MemoTestPredTyp::List,
        children: items,
        data: None,
    })
}

pub(crate) fn expr(data: Value) -> ArcPredNode<MemoTestRelTyp> {
    Arc::new(PredNode {
        typ: MemoTestPredTyp::Expr,
        children: vec![],
        data: Some(data),
    })
}

pub(crate) fn column_ref(col: &str) -> ArcPredNode<MemoTestRelTyp> {
    Arc::new(PredNode {
        typ: MemoTestPredTyp::ColumnRef,
        children: vec![],
        data: Some(Value::String(col.to_string().into())),
    })
}

pub(crate) fn group(group_id: GroupId) -> PlanNodeOrGroup<MemoTestRelTyp> {
    PlanNodeOrGroup::Group(group_id)
}
