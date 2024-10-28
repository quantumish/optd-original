use std::fmt::Display;

use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{
    ArcDfPlanNode, ArcDfPredNode, DfPredNode, DfPredType, DfReprPlanNode, DfReprPredNode,
};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum UnOpType {
    Neg = 1,
    Not,
}

impl Display for UnOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct UnOpPred(pub ArcDfPredNode);

impl UnOpPred {
    pub fn new(child: ArcDfPredNode, op_type: UnOpType) -> Self {
        UnOpPred(
            DfPredNode {
                typ: DfPredType::UnOp(op_type),
                children: vec![child.into_rel_node()],
                data: None,
            }
            .into(),
        )
    }

    pub fn child(&self) -> ArcDfPredNode {
        self.0.child(0)
    }

    pub fn op_type(&self) -> UnOpType {
        if let DfPredType::UnOp(op_type) = self.clone().into_rel_node().typ {
            op_type
        } else {
            panic!("not a un op")
        }
    }
}

impl DfReprPredNode for UnOpPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0.into_rel_node()
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::UnOp(_)) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            vec![self.child().explain(meta_map)],
        )
    }
}
