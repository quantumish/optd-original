use core::fmt;
use std::fmt::Display;

use pretty_xmlish::Pretty;

use optd_core::nodes::{PlanNode, PlanNodeMetaMap};

use super::{ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfReprPlanNode, JoinType};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ApplyType {
    Cross = 1,
    LeftOuter,
    Semi,
    AntiSemi,
}

impl Into<JoinType> for ApplyType {
    fn into(self) -> JoinType {
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
pub struct LogicalApply(pub ArcDfPlanNode);

impl DfReprPlanNode for LogicalApply {
    fn into_plan_node(self) -> ArcDfPlanNode {
        self.0.into_plan_node()
    }

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self> {
        if let DfNodeType::Apply(_) = plan_node.typ {
            Some(Self(plan_node))
        } else {
            None
        }
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
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
        left: ArcDfPlanNode,
        right: ArcDfPlanNode,
        cond: ArcDfPredNode,
        apply_type: ApplyType,
    ) -> LogicalApply {
        LogicalApply(
            DfPlanNode {
                typ: DfNodeType::Apply(apply_type),
                children: vec![left, right],
                predicates: vec![cond],
            }
            .into(),
        )
    }

    pub fn left_child(&self) -> ArcDfPlanNode {
        self.0.child(0)
    }

    pub fn right_child(&self) -> ArcDfPlanNode {
        self.0.child(1)
    }

    pub fn cond(&self) -> ArcDfPredNode {
        self.0.predicate(0)
    }

    pub fn apply_type(&self) -> ApplyType {
        if let DfNodeType::Apply(jty) = self.0 .0.typ {
            jty
        } else {
            unreachable!()
        }
    }
}
