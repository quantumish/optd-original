use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{node::PlanNode, optimizer::Optimizer};
use std::collections::HashMap;
use std::sync::Arc;

use crate::plan_nodes::{
    ConstantExpr, ConstantType, LogicalEmptyRelation, OptRelNode, OptRelNodeTyp,
};

use super::macros::define_rule;
use crate::properties::schema::SchemaPropertyBuilder;

define_rule!(
    EliminateLimitRule,
    apply_eliminate_limit,
    (Limit, child, [skip], [fetch])
);

/// Transformations:
///     - Limit with skip 0 and no fetch -> Eliminate from the tree
///     - Limit with limit 0 -> EmptyRelation
fn apply_eliminate_limit(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateLimitRulePicks { child, skip, fetch }: EliminateLimitRulePicks,
) -> Vec<PlanNode<OptRelNodeTyp>> {
    if let OptRelNodeTyp::Constant(ConstantType::UInt64) = skip.typ {
        if let OptRelNodeTyp::Constant(ConstantType::UInt64) = fetch.typ {
            let skip_val = ConstantExpr::from_rel_node(skip.into())
                .unwrap()
                .value()
                .as_u64();

            let fetch_val = ConstantExpr::from_rel_node(fetch.into())
                .unwrap()
                .value()
                .as_u64();

            // Bad convention to have u64 max represent None
            let fetch_is_none = fetch_val == u64::MAX;

            let schema =
                optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(child.clone()), 0);

            if fetch_is_none && skip_val == 0 {
                return vec![child];
            } else if fetch_val == 0 {
                let node = LogicalEmptyRelation::new(false, schema);
                return vec![node.into_rel_node().as_ref().clone()];
            }
        }
    }
    vec![]
}
