use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{nodes::PlanNode, optimizer::Optimizer};
use std::collections::HashMap;
use std::sync::Arc;

use crate::plan_nodes::{
    ConstantPred, ConstantType, DfNodeType, DfReprPlanNode, LogicalEmptyRelation,
};

use crate::properties::schema::SchemaPropertyBuilder;

use super::macros::define_rule;

define_rule!(
    EliminateLimitRule,
    apply_eliminate_limit,
    (Limit, [child], [skip, fetch])
);

/// Transformations:
///     - Limit with skip 0 and no fetch -> Eliminate from the tree
///     - Limit with limit 0 -> EmptyRelation
fn apply_eliminate_limit(
    optimizer: &impl Optimizer<DfNodeType>,
    EliminateLimitRulePicks { child, skip, fetch }: EliminateLimitRulePicks,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    if let DfNodeType::Constant(ConstantType::UInt64) = skip.typ {
        if let DfNodeType::Constant(ConstantType::UInt64) = fetch.typ {
            let skip_val = ConstantPred::from_rel_node(skip.into())
                .unwrap()
                .value()
                .as_u64();

            let fetch_val = ConstantPred::from_rel_node(fetch.into())
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
