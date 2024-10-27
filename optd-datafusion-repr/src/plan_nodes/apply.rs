use core::fmt;
use std::fmt::Display;

use pretty_xmlish::Pretty;

use optd_core::nodes::{PlanNode, PlanNodeMetaMap};

use super::{DfNodeType, DfReprPlanNode, Expr, JoinType, DfReprPlanNode, ArcDfPlanNode};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ApplyType {
    Cross = 1,
    LeftOuter,
    Semi,
    AntiSemi,
}

impl ApplyType {
    pub fn to_join_type(self) -> JoinType {
        match self {
            Self::Cross => JoinType::Cross,
            Self::LeftOuter => JoinType::LeftOuter,
            Self::Semi => JoinType::LeftSemi,
            Self::AntiSemi => JoinType::LeftAnti,
        }
    }
}

impl Display for ApplyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalApply(pub DfReprPlanNode);

impl DfReprPlanNode for LogicalApply {
    fn into_rel_node(self) -> ArcDfPlanNode {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: ArcDfPlanNode) -> Option<Self> {
        if let DfNodeType::Apply(_) = rel_node.typ {
            DfReprPlanNode::from_rel_node(rel_node).map(Self)
        } else {
            None
        }
    }

    fn dispatch_explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "LogicalApply",
            vec![
                ("typ", self.apply_type().to_string().into()),
                ("cond", self.cond().explain(meta_map)),
            ],
            vec![
                self.left_child().explain(meta_map),
                self.right_child().explain(meta_map),
            ],
        )
    }
}

impl LogicalApply {
    pub fn new(
        left: DfReprPlanNode,
        right: DfReprPlanNode,
        cond: Expr,
        apply_type: ApplyType,
    ) -> LogicalApply {
        LogicalApply(DfReprPlanNode(
            PlanNode {
                typ: DfNodeType::Apply(apply_type),
                children: vec![
                    left.into_rel_node(),
                    right.into_rel_node(),
                    cond.into_rel_node(),
                ],
                data: None,
            }
            .into(),
        ))
    }

    pub fn left_child(&self) -> DfReprPlanNode {
        DfReprPlanNode::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }

    pub fn right_child(&self) -> DfReprPlanNode {
        DfReprPlanNode::from_rel_node(self.clone().into_rel_node().child(1)).unwrap()
    }

    pub fn cond(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(2)).unwrap()
    }

    pub fn apply_type(&self) -> ApplyType {
        if let DfNodeType::Apply(jty) = self.0 .0.typ {
            jty
        } else {
            unreachable!()
        }
    }
}
