use optd_core::nodes::{PlanNode, PlanNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{DfNodeType, Expr, DfReprPlanNode, ArcDfPlanNode};

#[derive(Clone, Debug)]
pub struct ColumnRefExpr(pub Expr);

impl ColumnRefExpr {
    /// Creates a new `ColumnRef` expression.
    pub fn new(column_idx: usize) -> ColumnRefExpr {
        // this conversion is always safe since usize is at most u64
        let u64_column_idx = column_idx as u64;
        ColumnRefExpr(Expr(
            PlanNode {
                typ: DfNodeType::ColumnRef,
                children: vec![],
                data: Some(Value::UInt64(u64_column_idx)),
            }
            .into(),
        ))
    }

    fn get_data_usize(&self) -> usize {
        self.0 .0.data.as_ref().unwrap().as_u64() as usize
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        self.get_data_usize()
    }
}

impl DfReprPlanNode for ColumnRefExpr {
    fn into_rel_node(self) -> ArcDfPlanNode {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: ArcDfPlanNode) -> Option<Self> {
        if rel_node.typ != DfNodeType::ColumnRef {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&format!("#{}", self.index()))
    }
}
