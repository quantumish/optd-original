//! Typed interface of plan nodes.

mod agg;
mod apply;
mod empty_relation;
mod expr;
mod filter;
mod join;
mod limit;
pub(super) mod macros;
mod projection;
mod scan;
mod sort;
mod subquery;

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::DataType;
use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, GroupId},
    nodes::{
        ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeMeta, PlanNodeMetaMap, PredNode,
    },
    optimizer::Optimizer,
};

pub use agg::{LogicalAgg, PhysicalAgg};
pub use apply::{ApplyType, LogicalApply};
pub use empty_relation::{LogicalEmptyRelation, PhysicalEmptyRelation};
pub use expr::{
    BetweenExpr, BinOpExpr, BinOpType, CastExpr, ColumnRefExpr, ConstantExpr, ConstantType,
    DataTypeExpr, ExprList, FuncExpr, FuncType, InListExpr, LikeExpr, LogOpExpr, LogOpType,
    SortOrderExpr, SortOrderType, UnOpExpr, UnOpType,
};
pub use filter::{LogicalFilter, PhysicalFilter};
pub use join::{JoinType, LogicalJoin, PhysicalHashJoin, PhysicalNestedLoopJoin};
pub use limit::{LogicalLimit, PhysicalLimit};
use pretty_xmlish::{Pretty, PrettyConfig};
pub use projection::{LogicalProjection, PhysicalProjection};
pub use scan::{LogicalScan, PhysicalScan};
pub use sort::{LogicalSort, PhysicalSort};
pub use subquery::{DependentJoin, ExternColumnRefExpr, RawDependentJoin}; // Add missing import

use crate::properties::schema::{Schema, SchemaPropertyBuilder};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DfPredType {
    Constant(ConstantType),
    ColumnRef,
    ExternColumnRef,
    UnOp(UnOpType),
    BinOp(BinOpType),
    LogOp(LogOpType),
    Func(FuncType),
    SortOrder(SortOrderType),
    Between,
    Cast,
    Like,
    DataType(DataType),
    InList,
}

impl std::fmt::Display for DfPredType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// DfNodeType FAQ:
///   - The define_plan_node!() macro defines what the children of each join node are
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DfNodeType {
    // Developers: update `is_logical` function after adding new plan nodes
    // Plan nodes
    Projection,
    Filter,
    Scan,
    Join(JoinType),
    RawDepJoin(JoinType),
    DepJoin(JoinType),
    Sort,
    Agg,
    Apply(ApplyType),
    EmptyRelation,
    Limit,
    // Physical plan nodes
    PhysicalProjection,
    PhysicalFilter,
    PhysicalScan,
    PhysicalSort,
    PhysicalAgg,
    PhysicalHashJoin(JoinType),
    PhysicalNestedLoopJoin(JoinType),
    PhysicalEmptyRelation,
    PhysicalLimit,
}

impl std::fmt::Display for DfNodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl NodeType for DfNodeType {
    type PredType = DfPredType;
    fn is_logical(&self) -> bool {
        matches!(
            self,
            Self::Projection
                | Self::Filter
                | Self::Scan
                | Self::Join(_)
                | Self::Apply(_)
                | Self::Sort
                | Self::Agg
                | Self::EmptyRelation
                | Self::Limit
        )
    }
}

pub type DfPlanNode = PlanNode<DfNodeType>;
pub type ArcDfPlanNode = ArcPlanNode<DfNodeType>;

pub trait DfReprPlanNode: 'static + Clone {
    fn into_plan_node(self) -> ArcDfPlanNode;

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self>;

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static>;

    fn explain_to_string(&self, meta_map: Option<&PlanNodeMetaMap>) -> String {
        let mut config = PrettyConfig {
            need_boundaries: false,
            reduced_spaces: false,
            width: 300,
            ..Default::default()
        };
        let mut out = String::new();
        config.unicode(&mut out, &self.explain(meta_map));
        out
    }
}

pub type DfPredNode = PredNode<DfNodeType>;
pub type ArcDfPredNode = ArcPredNode<DfNodeType>;
