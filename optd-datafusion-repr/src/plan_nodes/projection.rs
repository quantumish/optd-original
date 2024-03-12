use optd_core::rel_node::RelNode;

use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{ColumnRefExpr, Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalProjection(pub PlanNode);

define_plan_node!(
    LogicalProjection : PlanNode,
    Projection, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalProjection(pub PlanNode);

define_plan_node!(
    PhysicalProjection : PlanNode,
    PhysicalProjection, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

pub struct ProjectionMapping {
    forward: Vec<usize>,
    _backward: Vec<Option<usize>>,
}

impl ProjectionMapping {
    pub fn build(mapping: Vec<usize>) -> Option<Self> {
        let mut backward = vec![];
        for (i, &x) in mapping.iter().enumerate() {
            if x >= backward.len() {
                backward.resize(x + 1, None);
            }
            backward[x] = Some(i);
        }
        Some(Self {
            forward: mapping,
            _backward: backward,
        })
    }

    pub fn projection_col_refers_to(&self, col: usize) -> usize {
        self.forward[col]
    }

    pub fn _original_col_maps_to(&self, col: usize) -> Option<usize> {
        self._backward[col]
    }

    pub fn rewrite_condition(
        &self,
        cond: Expr,
        left_schema_size: usize,
        projection_schema_size: usize,
    ) -> Expr {
        if cond.typ() == OptRelNodeTyp::ColumnRef {
            let col = ColumnRefExpr::from_rel_node(cond.into_rel_node()).unwrap();
            let idx = col.index();
            if idx < projection_schema_size {
                let col = self.projection_col_refers_to(col.index());
                return ColumnRefExpr::new(col).into_expr();
            } else {
                let col = col.index();
                return ColumnRefExpr::new(col - projection_schema_size + left_schema_size)
                    .into_expr();
            }
        }
        let expr = cond.into_rel_node();
        let mut children = Vec::with_capacity(expr.children.len());
        for child in &expr.children {
            children.push(
                self.rewrite_condition(
                    Expr::from_rel_node(child.clone()).unwrap(),
                    left_schema_size,
                    projection_schema_size,
                )
                .into_rel_node(),
            );
        }

        Expr::from_rel_node(
            RelNode {
                typ: expr.typ.clone(),
                children,
                data: expr.data.clone(),
            }
            .into(),
        )
        .unwrap()
    }
}

impl LogicalProjection {
    pub fn compute_column_mapping(&self) -> Option<ProjectionMapping> {
        let mut mapping = vec![];
        for expr in self.exprs().to_vec() {
            let col_expr = ColumnRefExpr::from_rel_node(expr.into_rel_node())?;
            mapping.push(col_expr.index());
        }
        ProjectionMapping::build(mapping)
    }
}
