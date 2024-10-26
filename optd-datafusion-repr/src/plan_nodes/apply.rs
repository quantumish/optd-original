use core::fmt;
use std::fmt::Display;

use pretty_xmlish::Pretty;

use optd_core::node::{PlanNode, PlanNodeMetaMap};

use super::{DfPlanNode, Expr, JoinType, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

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
pub struct LogicalApply(pub DfPlanNode);

impl OptRelNode for LogicalApply {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if let OptRelNodeTyp::Apply(_) = rel_node.typ {
            DfPlanNode::from_rel_node(rel_node).map(Self)
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
        left: DfPlanNode,
        right: DfPlanNode,
        cond: Expr,
        apply_type: ApplyType,
    ) -> LogicalApply {
        LogicalApply(DfPlanNode(
            PlanNode {
                typ: OptRelNodeTyp::Apply(apply_type),
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

    pub fn left_child(&self) -> DfPlanNode {
        DfPlanNode::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }

    pub fn right_child(&self) -> DfPlanNode {
        DfPlanNode::from_rel_node(self.clone().into_rel_node().child(1)).unwrap()
    }

    pub fn cond(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(2)).unwrap()
    }

    pub fn apply_type(&self) -> ApplyType {
        if let OptRelNodeTyp::Apply(jty) = self.0 .0.typ {
            jty
        } else {
            unreachable!()
        }
    }
}
