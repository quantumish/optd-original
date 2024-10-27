use crate::HashMap;
use crate::Rule;

use optd_core::rules::RuleMatcher;
use std::sync::Arc;
use std::vec;

use crate::rules::macros::define_rule;
use optd_core::nodes::PlanNode;
use optd_core::optimizer::Optimizer;

use super::project_transpose_common::ProjectionMapping;
use crate::plan_nodes::{
    ColumnRefExpr, DfNodeType, DfReprPlanNode, Expr, ExprList, JoinType, LogicalJoin,
    LogicalProjection, DfReprPlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;

// (Proj A) join B -> (Proj (A join B))
define_rule!(
    ProjectionPullUpJoin,
    apply_projection_pull_up_join,
    (
        Join(JoinType::Inner),
        (Projection, left, [list]),
        right,
        [cond]
    )
);

fn apply_projection_pull_up_join(
    optimizer: &impl Optimizer<DfNodeType>,
    ProjectionPullUpJoinPicks {
        left,
        right,
        list,
        cond,
    }: ProjectionPullUpJoinPicks,
) -> Vec<PlanNode<DfNodeType>> {
    let left = Arc::new(left.clone());
    let right = Arc::new(right.clone());

    let list = ExprList::from_rel_node(Arc::new(list)).unwrap();

    let projection = LogicalProjection::new(DfReprPlanNode::from_group(left.clone()), list.clone());

    let Some(mapping) = ProjectionMapping::build(&projection.exprs()) else {
        return vec![];
    };

    // TODO(chi): support capture projection node.
    let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(left.clone(), 0);
    let right_schema = optimizer.get_property::<SchemaPropertyBuilder>(right.clone(), 0);
    let mut new_projection_exprs = list.to_vec();
    for i in 0..right_schema.len() {
        let col: Expr = ColumnRefExpr::new(i + left_schema.len()).into_expr();
        new_projection_exprs.push(col);
    }
    let node = LogicalProjection::new(
        LogicalJoin::new(
            DfReprPlanNode::from_group(left),
            DfReprPlanNode::from_group(right),
            mapping.rewrite_join_cond(
                Expr::from_rel_node(Arc::new(cond)).unwrap(),
                left_schema.len(),
            ),
            JoinType::Inner,
        )
        .into_plan_node(),
        ExprList::new(new_projection_exprs),
    );
    vec![node.into_rel_node().as_ref().clone()]
}
