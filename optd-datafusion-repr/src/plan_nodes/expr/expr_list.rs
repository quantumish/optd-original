use itertools::Itertools;
use optd_core::nodes::{PlanNode, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{DfNodeType, Expr, DfReprPlanNode, ArcDfPlanNode};

#[derive(Clone, Debug)]
pub struct ExprList(ArcDfPlanNode);

impl ExprList {
    pub fn new(exprs: Vec<Expr>) -> Self {
        ExprList(
            PlanNode::new_list(exprs.into_iter().map(|x| x.into_rel_node()).collect_vec()).into(),
        )
    }

    /// Gets number of expressions in the list
    pub fn len(&self) -> usize {
        self.0.children.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.children.is_empty()
    }

    pub fn child(&self, idx: usize) -> Expr {
        Expr::from_rel_node(self.0.child(idx)).unwrap()
    }

    pub fn to_vec(&self) -> Vec<Expr> {
        self.0
            .children
            .iter()
            .map(|x| Expr::from_rel_node(x.clone()).unwrap())
            .collect_vec()
    }

    pub fn from_group(rel_node: ArcDfPlanNode) -> Self {
        Self(rel_node)
    }
}

impl DfReprPlanNode for ExprList {
    fn into_rel_node(self) -> ArcDfPlanNode {
        self.0.clone()
    }

    fn from_rel_node(rel_node: ArcDfPlanNode) -> Option<Self> {
        if rel_node.typ != DfNodeType::List {
            return None;
        }
        Some(ExprList(rel_node))
    }

    fn dispatch_explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::Array(
            (0..self.len())
                .map(|x| self.child(x).explain(meta_map))
                .collect_vec(),
        )
    }
}
