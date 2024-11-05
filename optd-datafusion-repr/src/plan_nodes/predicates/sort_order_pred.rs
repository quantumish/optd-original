use std::fmt::Display;

use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{
    dispatch_pred_explain, ArcDfPlanNode, ArcDfPredNode, DfPredNode, DfPredType, DfReprPlanNode,
    DfReprPredNode,
};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum SortOrderType {
    Asc,
    Desc,
}

impl Display for SortOrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct SortOrderPred(pub ArcDfPredNode);

impl SortOrderPred {
    pub fn new(order: SortOrderType, child: ArcDfPredNode) -> Self {
        SortOrderPred(
            DfPredNode {
                typ: DfPredType::SortOrder(order),
                children: vec![child],
                data: None,
            }
            .into(),
        )
    }

    pub fn child(&self) -> ArcDfPredNode {
        self.0.child(0)
    }

    pub fn order(&self) -> SortOrderType {
        if let DfPredType::SortOrder(order) = self.0.typ {
            order
        } else {
            panic!("not a sort order expr")
        }
    }
}

impl DfReprPredNode for SortOrderPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::SortOrder(_)) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "SortOrder",
            vec![("order", self.order().to_string().into())],
            vec![dispatch_pred_explain(self.child(), meta_map)],
        )
    }
}
