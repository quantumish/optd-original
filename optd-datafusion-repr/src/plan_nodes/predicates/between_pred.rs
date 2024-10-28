use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPlanNode, DfReprPredNode};

#[derive(Clone, Debug)]
pub struct BetweenPred(pub ArcDfPredNode);

impl BetweenPred {
    pub fn new(child: ArcDfPredNode, lower: ArcDfPredNode, upper: ArcDfPredNode) -> Self {
        BetweenPred(
            DfPredNode {
                typ: DfPredType::Between,
                children: vec![child, lower, upper],
                data: None,
            }
            .into(),
        )
    }

    pub fn child(&self) -> ArcDfPredNode {
        self.0.child(0)
    }

    pub fn lower(&self) -> ArcDfPredNode {
        self.0.child(1)
    }

    pub fn upper(&self) -> ArcDfPredNode {
        self.0.child(2)
    }
}

impl DfReprPredNode for BetweenPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::Between) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "Between",
            vec![
                ("child", self.child().explain(meta_map)),
                ("lower", self.lower().explain(meta_map)),
                ("upper", self.upper().explain(meta_map)),
            ],
            vec![],
        )
    }
}
