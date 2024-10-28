use itertools::Itertools;
use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{
    ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPredNode, DfPredType, DfReprPlanNode,
    DfReprPredNode,
};

#[derive(Clone, Debug)]
pub struct ListPred(pub ArcDfPredNode);

impl ListPred {
    pub fn new(preds: Vec<ArcDfPredNode>) -> Self {
        ListPred(DfPredNode {
            typ: DfPredType::List,
            children: preds,
            data: None,
        })
    }

    /// Gets number of expressions in the list
    pub fn len(&self) -> usize {
        self.0.children.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.children.is_empty()
    }

    pub fn child(&self, idx: usize) -> ArcDfPredNode {
        self.0.child(idx)
    }

    pub fn to_vec(&self) -> Vec<ArcDfPredNode> {
        self.0.children
    }
}

impl DfReprPredNode for ListPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if pred_node.typ != DfPredType::List {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::Array(
            (0..self.len())
                .map(|x| self.child(x).explain(meta_map))
                .collect_vec(),
        )
    }
}
