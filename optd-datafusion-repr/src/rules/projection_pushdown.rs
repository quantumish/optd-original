use std::collections::HashMap;
use std::sync::Arc;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    BetweenExpr, ColumnRefExpr, ExprList, LikeExpr, LogOpExpr, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};
use crate::properties::column_ref::ColumnRef;

use super::macros::define_rule;

// projects away aggregate calls that are not used
// TODO
define_rule!(
    ProjectAggregatePushDown,
    apply_projection_agg_pushdown,
    (
        Projection, 
        (Agg, child, [agg_exprs], [agg_groups]), 
        [exprs]
    )
);

fn apply_projection_agg_pushdown(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectAggregatePushDownPicks { child, agg_exprs, agg_groups, exprs }: ProjectAggregatePushDownPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {



    vec![]
}

// pushes projections through filters
// adds a projection node after a filter node 
// only keeping necessary columns (proj node exprs + filter col exprs))
// TODO
define_rule!(
    ProjectFilterPushDown,
    apply_projection_filter_pushdown,
    (
        Projection, 
        (Filter, child, [cond]), 
        [exprs]
    )
);

fn apply_projection_filter_pushdown(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectFilterPushDownPicks { child, cond, exprs }: ProjectFilterPushDownPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // get columns out of cond
    let cond_cols: Vec<ColumnRefExpr> = match cond.typ {
        OptRelNodeTyp::LogOp(_) => {
            // make a queue of some kind

            vec![]
        },
        OptRelNodeTyp::Between => { 
            let between_expr = BetweenExpr::from_rel_node(Arc::new(cond)).unwrap();
            let expr = between_expr.child();
            if expr.typ() != OptRelNodeTyp::ColumnRef {
                vec![]
            } else {
                let col = ColumnRefExpr::from_rel_node(expr.into_rel_node()).unwrap();
                vec![col]
            }
        },
        OptRelNodeTyp::Like => { 
            let like_expr = LikeExpr::from_rel_node(Arc::new(cond)).unwrap();
            let expr = like_expr.child();
            if expr.typ() != OptRelNodeTyp::ColumnRef {
                vec![]
            } else {
                let col = ColumnRefExpr::from_rel_node(expr.into_rel_node()).unwrap();
                vec![col]
            }
        },
        _ => vec![]
    };
    if cond_cols.is_empty() {
        return vec![];
    }

    // have column ref expressions of cond cols
    // bottom-most projection will have proj cols + filter cols as a set
    vec![]
}


// test cases for project merge
// create table t1 (v1 int, v2 int);
// explain select v1,v2 from (select v1,v2 from t1);

// create table t3 (v1 int, v2 int, v3 int);
// explain select v2,v3 from (select v1,v3,v2 from t3);
// explain select v1,v2,v3 from (select v1,v3,v2 from t3);

// Proj (Proj A) -> Proj A
// merges/removes projections
define_rule!(
    ProjectMergeRule,
    apply_projection_merge,
    (
        Projection, 
        (Projection, child, [exprs2]), 
        [exprs1]
    )
);

fn apply_projection_merge(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectMergeRulePicks { child, exprs1, exprs2 }: ProjectMergeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child = PlanNode::from_group(child.into());
    let exprs1 = ExprList::from_rel_node(exprs1.into()).unwrap();
    let exprs2 = ExprList::from_rel_node(exprs2.into()).unwrap();

    let Some(mapping) = LogicalProjection::compute_column_mapping(&exprs1) else {
        return vec![];
    };

    let res_exprs = mapping.rewrite_projection(&exprs2);

    let node: LogicalProjection = LogicalProjection::new(
        child,
        res_exprs,
    );
    vec![node.into_rel_node().as_ref().clone()]
}
