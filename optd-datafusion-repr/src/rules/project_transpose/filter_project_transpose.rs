use std::collections::HashMap;
use std::vec;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    Expr, ExprList, LogicalFilter, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};

use crate::rules::macros::define_rule;
use crate::rules::project_transpose::project_transpose_common::ProjectionMapping;

define_rule!(
    FilterProjectTransposeRule,
    apply_filter_project_transpose,
    (Filter, (Projection, child, [exprs]), [cond])
);

/// Datafusion only pushes filter past project when the project does not contain
/// volatile (i.e. non-deterministic) expressions that are present in the filter
/// Calcite only checks if the projection contains a windowing calculation
/// We check neither of those things and do it always (which may be wrong)
fn apply_filter_project_transpose(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterProjectTransposeRulePicks { child, exprs, cond }: FilterProjectTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child = PlanNode::from_group(child.into());
    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();
    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();

    let proj_col_map = ProjectionMapping::build(&exprs).unwrap();
    let rewritten_cond = proj_col_map.rewrite_filter_cond(cond_as_expr.clone(), false);

    let new_filter_node = LogicalFilter::new(child, rewritten_cond);
    let new_proj = LogicalProjection::new(new_filter_node.into_plan_node(), exprs);
    vec![new_proj.into_rel_node().as_ref().clone()]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optd_core::optimizer::Optimizer;

    use crate::{
        plan_nodes::{
            BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ExprList, LogOpExpr, LogOpType, 
            LogicalFilter, LogicalProjection, LogicalScan, OptRelNode, OptRelNodeTyp
        },
        rules::FilterProjectTransposeRule,
        testing::new_test_optimizer,
    };

    #[test]
    fn push_past_proj_basic() {
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterProjectTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let proj = LogicalProjection::new(scan.into_plan_node(), ExprList::new(vec![ColumnRefExpr::new(0).into_expr()]));

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let filter = LogicalFilter::new(proj.into_plan_node(), filter_expr);
        let plan = test_optimizer.optimize(filter.into_rel_node()).unwrap();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Filter));
    }

    #[test]
    fn push_past_proj_adv() {
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterProjectTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let proj = LogicalProjection::new(
            scan.into_plan_node(),
            ExprList::new(vec![
                ColumnRefExpr::new(0).into_expr(),
                ColumnRefExpr::new(4).into_expr(),
                ColumnRefExpr::new(5).into_expr(),
                ColumnRefExpr::new(7).into_expr(),
            ]),
        );

        let filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    // This one should be pushed to the left child
                    ColumnRefExpr::new(1).into_expr(),
                    ConstantExpr::int32(5).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // This one should be pushed to the right child
                    ColumnRefExpr::new(3).into_expr(),
                    ConstantExpr::int32(6).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        );

        let filter = LogicalFilter::new(proj.into_plan_node(), filter_expr.into_expr());

        let plan = test_optimizer.optimize(filter.into_rel_node()).unwrap();

        assert!(matches!(plan.typ, OptRelNodeTyp::Projection));
        let plan_filter = LogicalFilter::from_rel_node(plan.child(0)).unwrap();
        assert!(matches!(plan_filter.0.typ(), OptRelNodeTyp::Filter));
        let plan_filter_expr =
            LogOpExpr::from_rel_node(plan_filter.cond().into_rel_node()).unwrap();
        assert!(matches!(plan_filter_expr.op_type(), LogOpType::And));
        let op_0 = BinOpExpr::from_rel_node(plan_filter_expr.children()[0].clone().into_rel_node())
            .unwrap();
        let col_0 =
            ColumnRefExpr::from_rel_node(op_0.left_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_0.index(), 4);
        let op_1 = BinOpExpr::from_rel_node(plan_filter_expr.children()[1].clone().into_rel_node())
            .unwrap();
        let col_1 =
            ColumnRefExpr::from_rel_node(op_1.left_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_1.index(), 7);
    }

}