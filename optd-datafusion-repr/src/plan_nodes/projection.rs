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

/// This struct holds the mapping from original columns to projected columns.
///
/// # Example
/// With the following plan:
///  | Filter (#0 < 5)
///  |
///  |-| Projection [#2, #3]
///    |- Scan [#0, #1, #2, #3]
///
/// The computed projection mapping is:
/// #2 -> #0
/// #3 -> #1
#[derive(Clone, Debug)]
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

    /// Recursively rewrites all ColumnRefs in an Expr to *undo* the projection
    /// condition. You might want to do this if you are pushing something
    /// through a projection, or pulling a projection up.
    ///
    /// # Example
    /// If we have a projection node, mapping column A to column B (A -> B)
    /// All B's in `cond` will be rewritten as A.
    pub fn rewrite_condition(&self, cond: Expr, child_schema_len: usize) -> Expr {
        let proj_schema_size = self.forward.len();
        cond.rewrite_column_refs(&|idx| {
            Some(if idx < proj_schema_size {
                self.projection_col_refers_to(idx)
            } else {
                idx - proj_schema_size + child_schema_len
            })
        })
        .unwrap()
    }

    /// Recursively rewrites all ColumnRefs in an Expr to what the projection
    /// node is rewriting. E.g. if Projection is A -> B, B will be rewritten as A
    pub fn reverse_rewrite_condition(&self, cond: Expr) -> Expr {
        let proj_schema_size = self._backward.len();
        cond.rewrite_column_refs(&|idx| {
            Some(if idx < proj_schema_size {
                self._original_col_maps_to(idx).unwrap()
            } else {
                panic!("exprs do not map to projection");
            })
        })
        .unwrap()
    }

    /// Rewrites all ColumnRefs in an ExprList to what the projection
    /// node is rewriting. E.g. if Projection is A -> B, B will be 
    /// rewritten as A
    pub fn rewrite_projection(&self, exprs: &ExprList) -> Option<ExprList> {
        if exprs.len() == 0 {
            return None;
        }
        let mut new_projection_exprs = Vec::new();
        let exprs = exprs.to_vec();
        for i in &self.forward {
            let col: Expr = exprs[*i].clone();
            new_projection_exprs.push(col);
        };
        Some(ExprList::new(new_projection_exprs))
    }

    /// rewrites the input exprs based on the mapped col refs
    /// intended use: 
    /// Projection { exprs: [#1, #0] }
    ///     Projection { exprs: [#0, #2] }
    /// remove bottom projection by converting nodes to:
    /// Projection { exprs: [#2, #0] }
    pub fn reverse_rewrite_projection(&self, exprs: &ExprList) -> ExprList {
        let mut new_projection_exprs = Vec::new();
        let exprs = exprs.to_vec();
        for i in 0..exprs.len() {
            let col: Expr = ColumnRefExpr::new(self.projection_col_refers_to(i).clone()).into_expr();
            new_projection_exprs.push(col);
        };
        ExprList::new(new_projection_exprs)
    }
}

impl LogicalProjection {
    pub fn compute_column_mapping(exprs: &ExprList) -> Option<ProjectionMapping> {
        let mut mapping = vec![];
        for expr in exprs.to_vec() {
            let col_expr = ColumnRefExpr::from_rel_node(expr.into_rel_node())?;
            mapping.push(col_expr.index());
        }
        ProjectionMapping::build(mapping)
    }
}
