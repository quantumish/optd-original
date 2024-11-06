use optd_core::nodes::{PlanNode, PlanNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{
    ArcDfPlanNode, ArcDfPredNode, DfPredNode, DfPredType, DfReprPlanNode, DfReprPredNode,
};

#[derive(Clone, Debug)]
pub struct ColumnRefPred(pub ArcDfPredNode);

impl ColumnRefPred {
    /// Creates a new `ColumnRef` expression.
    pub fn new(column_idx: usize) -> ColumnRefPred {
        // this conversion is always safe since usize is at most u64
        let u64_column_idx = column_idx as u64;
        ColumnRefPred(
            DfPredNode {
                typ: DfPredType::ColumnRef,
                children: vec![],
                data: Some(Value::UInt64(u64_column_idx)),
            }
            .into(),
        )
    }

    fn get_data_usize(&self) -> usize {
        self.0.data.as_ref().unwrap().as_u64() as usize
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        self.get_data_usize()
    }
}

impl DfReprPredNode for ColumnRefPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if pred_node.typ != DfPredType::ColumnRef {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&format!("#{}", self.index()))
    }
}
