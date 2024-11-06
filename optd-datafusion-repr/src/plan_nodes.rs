//! Typed interface of plan nodes.

mod agg;
mod empty_relation;
mod filter;
mod join;
mod limit;
pub(super) mod macros;
mod predicates;
mod projection;
mod scan;
mod sort;
mod subquery;

use std::fmt::Debug;

use arrow_schema::DataType;
use optd_core::nodes::{
    ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeMeta, PlanNodeMetaMap, PlanNodeOrGroup,
    PredNode,
};

pub use agg::{LogicalAgg, PhysicalAgg};
pub use empty_relation::{
    decode_empty_relation_schema, LogicalEmptyRelation, PhysicalEmptyRelation,
};
pub use filter::{LogicalFilter, PhysicalFilter};
pub use join::{JoinType, LogicalJoin, PhysicalHashJoin, PhysicalNestedLoopJoin};
pub use limit::{LogicalLimit, PhysicalLimit};
pub use predicates::{
    BetweenPred, BinOpPred, BinOpType, CastPred, ColumnRefPred, ConstantPred, ConstantType,
    DataTypePred, ExternColumnRefPred, FuncPred, FuncType, InListPred, LikePred, ListPred,
    LogOpPred, LogOpType, SortOrderPred, SortOrderType, UnOpPred, UnOpType,
};
use pretty_xmlish::{Pretty, PrettyConfig};
pub use projection::{LogicalProjection, PhysicalProjection};
pub use scan::{LogicalScan, PhysicalScan};
pub use sort::{LogicalSort, PhysicalSort};
pub use subquery::{DependentJoin, RawDependentJoin}; // Add missing import

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DfPredType {
    List,
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
                | Self::Sort
                | Self::Agg
                | Self::EmptyRelation
                | Self::Limit
        )
    }
}

pub type DfPlanNode = PlanNode<DfNodeType>;
pub type DfPlanNodeOrGroup = PlanNodeOrGroup<DfNodeType>;
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

fn get_meta<'a>(node: &ArcPlanNode<DfNodeType>, meta_map: &'a PlanNodeMetaMap) -> &'a PlanNodeMeta {
    meta_map.get(&(node as *const _ as usize)).unwrap()
}

pub fn dispatch_plan_explain(
    plan_node_maybe: DfPlanNodeOrGroup,
    meta_map: Option<&PlanNodeMetaMap>,
) -> Pretty<'static> {
    let DfPlanNodeOrGroup::PlanNode(plan_node) = plan_node_maybe else {
        unreachable!("Should not explain a placeholder");
    };
    match plan_node.typ {
        DfNodeType::Join(_) => LogicalJoin::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::RawDepJoin(_) => RawDependentJoin::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::DepJoin(_) => DependentJoin::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Scan => LogicalScan::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Filter => LogicalFilter::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::EmptyRelation => LogicalEmptyRelation::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Limit => LogicalLimit::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalFilter => PhysicalFilter::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalScan => PhysicalScan::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalNestedLoopJoin(_) => PhysicalNestedLoopJoin::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Agg => LogicalAgg::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Sort => LogicalSort::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Projection => LogicalProjection::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalProjection => PhysicalProjection::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalAgg => PhysicalAgg::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalSort => PhysicalSort::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalHashJoin(_) => PhysicalHashJoin::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalEmptyRelation => PhysicalEmptyRelation::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalLimit => PhysicalLimit::from_plan_node(plan_node)
            .unwrap()
            .explain(meta_map),
    }
}

pub fn dispatch_plan_explain_to_string(
    plan_node: DfPlanNodeOrGroup,
    meta_map: Option<&PlanNodeMetaMap>,
) -> String {
    let mut config = PrettyConfig {
        need_boundaries: false,
        reduced_spaces: false,
        width: 300,
        ..Default::default()
    };
    let mut out = String::new();
    config.unicode(&mut out, &dispatch_plan_explain(plan_node, meta_map));
    out
}

pub fn dispatch_pred_explain(
    pred_node: ArcDfPredNode,
    meta_map: Option<&PlanNodeMetaMap>,
) -> Pretty<'static> {
    match &pred_node.typ {
        DfPredType::Constant(_) => ConstantPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::ColumnRef => ColumnRefPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::ExternColumnRef => ExternColumnRefPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::List => ListPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::UnOp(_) => UnOpPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::BinOp(_) => BinOpPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::LogOp(_) => LogOpPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::Func(_) => FuncPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::SortOrder(_) => SortOrderPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::Between => BetweenPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::Cast => CastPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::Like => LikePred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::DataType(_) => DataTypePred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
        DfPredType::InList => InListPred::from_pred_node(pred_node)
            .unwrap()
            .explain(meta_map),
    }
}

pub fn dispatch_pred_explain_to_string(
    pred_node: ArcDfPredNode,
    meta_map: Option<&PlanNodeMetaMap>,
) -> String {
    let mut config = PrettyConfig {
        need_boundaries: false,
        reduced_spaces: false,
        width: 300,
        ..Default::default()
    };
    let mut out = String::new();
    config.unicode(&mut out, &dispatch_pred_explain(pred_node, meta_map));
    out
}

pub type DfPredNode = PredNode<DfNodeType>;
pub type ArcDfPredNode = ArcPredNode<DfNodeType>;

pub trait DfReprPredNode: 'static + Clone {
    fn into_pred_node(self) -> ArcDfPredNode;

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self>;

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static>;
}
