// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_og_datafusion_repr::plan_nodes::{
    ColumnRefPred, ConstantPred, DfPredType, DfReprPredNode, InListPred,
};
use optd_og_datafusion_repr::properties::column_ref::{
    BaseTableColumnRef, BaseTableColumnRefs, ColumnRef,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::adv_stats::stats::{Distribution, MostCommonValues};
use crate::adv_stats::{AdvStats, UNIMPLEMENTED_SEL};

impl<
        M: MostCommonValues + Clone + Serialize + DeserializeOwned,
        D: Distribution + Clone + Serialize + DeserializeOwned,
    > AdvStats<M, D>
{
    /// Only support colA in (val1, val2, val3) where colA is a column ref and
    /// val1, val2, val3 are constants.
    pub(super) fn get_in_list_selectivity(
        &self,
        expr: &InListPred,
        column_refs: &BaseTableColumnRefs,
    ) -> f64 {
        let child = expr.child();

        // Check child is a column ref.
        if !matches!(child.typ, DfPredType::ColumnRef) {
            return UNIMPLEMENTED_SEL;
        }

        // Check all expressions in the list are constants.
        let list_exprs = expr.list().to_vec();
        if list_exprs
            .iter()
            .any(|expr| !matches!(expr.typ, DfPredType::Constant(_)))
        {
            return UNIMPLEMENTED_SEL;
        }

        // Convert child and const expressions to concrete types.
        let col_ref_idx = ColumnRefPred::from_pred_node(child).unwrap().index();
        let list_exprs = list_exprs
            .into_iter()
            .map(|expr| {
                ConstantPred::from_pred_node(expr)
                    .expect("we already checked all list elements are constants")
            })
            .collect::<Vec<_>>();
        let negated = expr.negated();

        if let ColumnRef::BaseTableColumnRef(BaseTableColumnRef { table, col_idx }) =
            &column_refs[col_ref_idx]
        {
            let in_sel = list_exprs
                .iter()
                .map(|expr| {
                    self.get_column_equality_selectivity(table, *col_idx, &expr.value(), true)
                })
                .sum::<f64>()
                .min(1.0);
            if negated {
                1.0 - in_sel
            } else {
                in_sel
            }
        } else {
            // Child is a derived column.
            UNIMPLEMENTED_SEL
        }
    }
}

#[cfg(test)]
mod tests {
    use optd_og_datafusion_repr::properties::column_ref::ColumnRef;
    use optd_og_datafusion_repr::Value;

    use crate::adv_stats::tests::{
        create_one_column_cost_model, in_list, TestDistribution, TestMostCommonValues,
        TestPerColumnStats, TABLE1_NAME,
    };

    #[test]
    fn test_in_list() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.8), (Value::Int32(2), 0.2)]),
            2,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model
                .get_in_list_selectivity(&in_list(0, vec![Value::Int32(1)], false), &column_refs),
            0.8
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_in_list_selectivity(
                &in_list(0, vec![Value::Int32(1), Value::Int32(2)], false),
                &column_refs
            ),
            1.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model
                .get_in_list_selectivity(&in_list(0, vec![Value::Int32(3)], false), &column_refs),
            0.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model
                .get_in_list_selectivity(&in_list(0, vec![Value::Int32(1)], true), &column_refs),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_in_list_selectivity(
                &in_list(0, vec![Value::Int32(1), Value::Int32(2)], true),
                &column_refs
            ),
            0.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model
                .get_in_list_selectivity(&in_list(0, vec![Value::Int32(3)], true), &column_refs),
            1.0
        );
    }
}
