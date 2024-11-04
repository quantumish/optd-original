use optd_core::{
    cascades::{BindingType, CascadesOptimizer, RelNodeContext},
    cost::Cost,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::adv_cost::{
    stats::{Distribution, MostCommonValues},
    DEFAULT_NUM_DISTINCT,
};
use optd_datafusion_repr::{
    plan_nodes::{ExprList, OptRelNode, OptRelNodeTyp},
    properties::column_ref::{BaseTableColumnRef, ColumnRef, ColumnRefPropertyBuilder},
};

use super::{OptCostModel, DEFAULT_UNK_SEL};

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > OptCostModel<M, D>
{
    pub(super) fn get_agg_cost(
        &self,
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType>>,
    ) -> Cost {
        let child_row_cnt = Self::row_cnt(&children[0]);
        let row_cnt = self.get_agg_row_cnt(context, optimizer, child_row_cnt);
        let (_, compute_cost_1, _) = Self::cost_tuple(&children[1]);
        let (_, compute_cost_2, _) = Self::cost_tuple(&children[2]);
        Self::cost(
            row_cnt,
            child_row_cnt * (compute_cost_1 + compute_cost_2),
            0.0,
        )
    }

    fn get_agg_row_cnt(
        &self,
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType>>,
        child_row_cnt: f64,
    ) -> f64 {
        if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let group_by_id = context.children_group_ids[2];
            let group_by = optimizer
                .get_predicate_binding(group_by_id)
                .expect("no expression found?");
            let group_by = ExprList::from_rel_node(group_by).unwrap();
            if group_by.is_empty() {
                1.0
            } else {
                // Multiply the n-distinct of all the group by columns.
                // TODO: improve with multi-dimensional n-distinct
                let group_col_refs = optimizer
                    .get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
                group_col_refs
                    .base_table_column_refs()
                    .iter()
                    .take(group_by.len())
                    .map(|col_ref| match col_ref {
                        ColumnRef::BaseTableColumnRef(BaseTableColumnRef { table, col_idx }) => {
                            let table_stats = self.per_table_stats_map.get(table);
                            let column_stats = table_stats.and_then(|table_stats| {
                                table_stats.column_comb_stats.get(&vec![*col_idx])
                            });

                            if let Some(column_stats) = column_stats {
                                column_stats.ndistinct as f64
                            } else {
                                // The column type is not supported or stats are missing.
                                DEFAULT_NUM_DISTINCT as f64
                            }
                        }
                        ColumnRef::Derived => DEFAULT_NUM_DISTINCT as f64,
                        _ => panic!(
                            "GROUP BY base table column ref must either be derived or base table"
                        ),
                    })
                    .product()
            }
        } else {
            (child_row_cnt * DEFAULT_UNK_SEL).max(1.0)
        }
    }
}
