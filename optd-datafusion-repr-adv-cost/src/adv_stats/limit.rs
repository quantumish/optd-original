use optd_core::{
    cascades::{BindingType, CascadesOptimizer, RelNodeContext},
    cost::Cost,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::adv_cost::stats::{Distribution, MostCommonValues};
use optd_datafusion_repr::plan_nodes::{ConstantExpr, ConstantType, OptRelNode, OptRelNodeTyp};

use super::{OptCostModel, DEFAULT_UNK_SEL};

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > OptCostModel<M, D>
{
    pub(super) fn get_limit_cost(
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType>>,
    ) -> Cost {
        let (row_cnt, compute_cost, _) = Self::cost_tuple(&children[0]);
        let row_cnt = if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let fetch_expr = optimizer
                .get_predicate_binding(context.children_group_ids[2])
                .expect("no expression found?");
            assert!(
                matches!(fetch_expr.typ, DfNodeType::Constant(ConstantType::UInt64)),
                "fetch type can only be UInt64"
            );
            let fetch = ConstantPred::from_rel_node(fetch_expr)
                .unwrap()
                .value()
                .as_u64();
            // u64::MAX represents None
            if fetch == u64::MAX {
                row_cnt
            } else {
                row_cnt.min(fetch as f64)
            }
        } else {
            (row_cnt * DEFAULT_UNK_SEL).max(1.0)
        };
        Self::cost(row_cnt, compute_cost, 0.0)
    }
}
