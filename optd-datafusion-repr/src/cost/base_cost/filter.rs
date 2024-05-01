use std::ops::Bound;

use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::Cost,
    rel_node::Value,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    cost::base_cost::{
        stats::{ColumnCombValueStats, Distribution, MostCommonValues},
        UNIMPLEMENTED_SEL,
    },
    plan_nodes::{
        BinOpType, CastExpr, ColumnRefExpr, ConstantExpr, ConstantType, Expr, InListExpr, LikeExpr,
        LogOpType, OptRelNode, OptRelNodeRef, OptRelNodeTyp, UnOpType,
    },
    properties::{
        column_ref::{
            BaseTableColumnRef, BaseTableColumnRefs, ColumnRef, ColumnRefPropertyBuilder,
        },
        schema::{Schema, SchemaPropertyBuilder},
    },
};

use super::{
    stats::ColumnCombValue, OptCostModel, DEFAULT_EQ_SEL, DEFAULT_INEQ_SEL, DEFAULT_UNK_SEL,
};

mod in_list;
mod like;

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > OptCostModel<M, D>
{
    pub(super) fn get_filter_cost(
        &self,
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
        let (_, compute_cost, _) = Self::cost_tuple(&children[1]);
        let selectivity = if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let schema =
                optimizer.get_property_by_group::<SchemaPropertyBuilder>(context.group_id, 0);
            let column_refs =
                optimizer.get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
            let column_refs = column_refs.base_table_column_refs();
            let expr_group_id = context.children_group_ids[1];
            let expr_trees = optimizer.get_all_group_bindings(expr_group_id, false);
            // there may be more than one expression tree in a group (you can see this trivially as you can just swap the order of two subtrees for commutative operators)
            // however, we just take an arbitrary expression tree from the group to compute selectivity
            let expr_tree = expr_trees.first().expect("expression missing");
            self.get_filter_selectivity(expr_tree.clone(), &schema, column_refs)
        } else {
            DEFAULT_UNK_SEL
        };
        Self::cost(
            (row_cnt * selectivity).max(1.0),
            row_cnt * compute_cost,
            0.0,
        )
    }

    /// The expr_tree input must be a "mixed expression tree".
    ///
    /// - An "expression node" refers to a RelNode that returns true for is_expression()
    /// - A "full expression tree" is where every node in the tree is an expression node
    /// - A "mixed expression tree" is where every base-case node and all its parents are expression nodes
    /// - A "base-case node" is a node that doesn't lead to further recursion (such as a BinOp(Eq))
    ///
    /// The schema input is the schema the predicate represented by the expr_tree is applied on.
    ///
    /// The output will be the selectivity of the expression tree if it were a "filter predicate".
    ///
    /// A "filter predicate" operates on one input node, unlike a "join predicate" which operates on two input nodes.
    /// This is why the function only takes in a single schema.
    pub(super) fn get_filter_selectivity(
        &self,
        expr_tree: OptRelNodeRef,
        schema: &Schema,
        column_refs: &BaseTableColumnRefs,
    ) -> f64 {
        assert!(expr_tree.typ.is_expression());
        match &expr_tree.typ {
            OptRelNodeTyp::Constant(_) => Self::get_constant_selectivity(expr_tree),
            OptRelNodeTyp::ColumnRef => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::UnOp(un_op_typ) => {
                assert!(expr_tree.children.len() == 1);
                let child = expr_tree.child(0);
                match un_op_typ {
                    // not doesn't care about nulls so there's no complex logic. it just reverses the selectivity
                    // for instance, != _will not_ include nulls but "NOT ==" _will_ include nulls
                    UnOpType::Not => 1.0 - self.get_filter_selectivity(child, schema, column_refs),
                    UnOpType::Neg => panic!(
                        "the selectivity of operations that return numerical values is undefined"
                    ),
                }
            }
            OptRelNodeTyp::BinOp(bin_op_typ) => {
                assert!(expr_tree.children.len() == 2);
                let left_child = expr_tree.child(0);
                let right_child = expr_tree.child(1);

                if bin_op_typ.is_comparison() {
                    self.get_comp_op_selectivity(
                        *bin_op_typ,
                        left_child,
                        right_child,
                        schema,
                        column_refs,
                    )
                } else if bin_op_typ.is_numerical() {
                    panic!(
                        "the selectivity of operations that return numerical values is undefined"
                    )
                } else {
                    unreachable!("all BinOpTypes should be true for at least one is_*() function")
                }
            }
            OptRelNodeTyp::LogOp(log_op_typ) => {
                self.get_log_op_selectivity(*log_op_typ, &expr_tree.children, schema, column_refs)
            }
            OptRelNodeTyp::Func(_) => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::SortOrder(_) => {
                panic!("the selectivity of sort order expressions is undefined")
            }
            OptRelNodeTyp::Between => UNIMPLEMENTED_SEL,
            OptRelNodeTyp::Cast => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::Like => {
                let like_expr = LikeExpr::from_rel_node(expr_tree).unwrap();
                self.get_like_selectivity(&like_expr, column_refs)
            }
            OptRelNodeTyp::DataType(_) => {
                panic!("the selectivity of a data type is not defined")
            }
            OptRelNodeTyp::InList => {
                let in_list_expr = InListExpr::from_rel_node(expr_tree).unwrap();
                self.get_in_list_selectivity(&in_list_expr, column_refs)
            }
            _ => unreachable!(
                "all expression OptRelNodeTyp were enumerated. this should be unreachable"
            ),
        }
    }

    fn get_constant_selectivity(const_node: OptRelNodeRef) -> f64 {
        if let OptRelNodeTyp::Constant(const_typ) = const_node.typ {
            if matches!(const_typ, ConstantType::Bool) {
                let value = const_node
                    .as_ref()
                    .data
                    .as_ref()
                    .expect("constants should have data");
                if let Value::Bool(bool_value) = value {
                    if *bool_value {
                        1.0
                    } else {
                        0.0
                    }
                } else {
                    unreachable!(
                        "if the typ is ConstantType::Bool, the value should be a Value::Bool"
                    )
                }
            } else {
                panic!("selectivity is not defined on constants which are not bools")
            }
        } else {
            panic!("get_constant_selectivity must be called on a constant")
        }
    }

    fn get_log_op_selectivity(
        &self,
        log_op_typ: LogOpType,
        children: &[OptRelNodeRef],
        schema: &Schema,
        column_refs: &BaseTableColumnRefs,
    ) -> f64 {
        let children_sel = children
            .iter()
            .map(|expr| self.get_filter_selectivity(expr.clone(), schema, column_refs));

        match log_op_typ {
            LogOpType::And => children_sel.product(),
            // the formula is 1.0 - the probability of _none_ of the events happening
            LogOpType::Or => 1.0 - children_sel.fold(1.0, |acc, sel| acc * (1.0 - sel)),
        }
    }

    /// Convert the left and right child nodes of some operation to what they semantically are.
    /// This is convenient to avoid repeating the same logic just with "left" and "right" swapped.
    /// The last return value is true when the input node (left) is a ColumnRefExpr.
    fn get_semantic_nodes(
        left: OptRelNodeRef,
        right: OptRelNodeRef,
        schema: &Schema,
    ) -> (Vec<ColumnRefExpr>, Vec<Value>, Vec<OptRelNodeRef>, bool) {
        let mut col_ref_exprs = vec![];
        let mut values = vec![];
        let mut non_col_ref_exprs = vec![];
        let is_left_col_ref;

        // Recursively unwrap casts as much as we can.
        let mut uncasted_left = left;
        let mut uncasted_right = right;
        loop {
            // println!("loop {}, uncasted_left={:?}, uncasted_right={:?}", Local::now(), uncasted_left, uncasted_right);
            if uncasted_left.as_ref().typ == OptRelNodeTyp::Cast
                && uncasted_right.as_ref().typ == OptRelNodeTyp::Cast
            {
                let left_cast_expr = CastExpr::from_rel_node(uncasted_left)
                    .expect("we already checked that the type is Cast");
                let right_cast_expr = CastExpr::from_rel_node(uncasted_right)
                    .expect("we already checked that the type is Cast");
                assert!(left_cast_expr.cast_to() == right_cast_expr.cast_to());
                uncasted_left = left_cast_expr.child().into_rel_node();
                uncasted_right = right_cast_expr.child().into_rel_node();
            } else if uncasted_left.as_ref().typ == OptRelNodeTyp::Cast
                || uncasted_right.as_ref().typ == OptRelNodeTyp::Cast
            {
                let is_left_cast = uncasted_left.as_ref().typ == OptRelNodeTyp::Cast;
                let (mut cast_node, mut non_cast_node) = if is_left_cast {
                    (uncasted_left, uncasted_right)
                } else {
                    (uncasted_right, uncasted_left)
                };

                let cast_expr = CastExpr::from_rel_node(cast_node)
                    .expect("we already checked that the type is Cast");
                let cast_expr_child = cast_expr.child().into_rel_node();
                let cast_expr_cast_to = cast_expr.cast_to();

                let should_break = match cast_expr_child.typ {
                    OptRelNodeTyp::Constant(_) => {
                        cast_node = ConstantExpr::new(
                            ConstantExpr::from_rel_node(cast_expr_child)
                                .expect("we already checked that the type is Constant")
                                .value()
                                .convert_to_type(cast_expr_cast_to),
                        )
                        .into_rel_node();
                        false
                    }
                    OptRelNodeTyp::ColumnRef => {
                        let col_ref_expr = ColumnRefExpr::from_rel_node(cast_expr_child)
                            .expect("we already checked that the type is ColumnRef");
                        let col_ref_idx = col_ref_expr.index();
                        cast_node = col_ref_expr.into_rel_node();
                        // The "invert" cast is to invert the cast so that we're casting the non_cast_node to
                        // the column's original type.
                        let invert_cast_data_type =
                            &schema.fields[col_ref_idx].typ.into_data_type();

                        match non_cast_node.typ {
                            OptRelNodeTyp::ColumnRef => {
                                // In general, there's no way to remove the Cast here. We can't move the Cast to the
                                // other ColumnRef because that would lead to an infinite loop. Thus, we just leave the
                                // cast where it is and break.
                                true
                            }
                            _ => {
                                non_cast_node = CastExpr::new(
                                    Expr::from_rel_node(non_cast_node).unwrap(),
                                    invert_cast_data_type.clone(),
                                )
                                .into_rel_node();
                                false
                            }
                        }
                    }
                    _ => todo!(),
                };

                (uncasted_left, uncasted_right) = if is_left_cast {
                    (cast_node, non_cast_node)
                } else {
                    (non_cast_node, cast_node)
                };

                if should_break {
                    break;
                }
            } else {
                break;
            }
        }

        // Sort nodes into col_ref_exprs, values, and non_col_ref_exprs
        match uncasted_left.as_ref().typ {
            OptRelNodeTyp::ColumnRef => {
                is_left_col_ref = true;
                col_ref_exprs.push(
                    ColumnRefExpr::from_rel_node(uncasted_left)
                        .expect("we already checked that the type is ColumnRef"),
                );
            }
            OptRelNodeTyp::Constant(_) => {
                is_left_col_ref = false;
                values.push(
                    ConstantExpr::from_rel_node(uncasted_left)
                        .expect("we already checked that the type is Constant")
                        .value(),
                )
            }
            _ => {
                is_left_col_ref = false;
                non_col_ref_exprs.push(uncasted_left);
            }
        }
        match uncasted_right.as_ref().typ {
            OptRelNodeTyp::ColumnRef => {
                col_ref_exprs.push(
                    ColumnRefExpr::from_rel_node(uncasted_right)
                        .expect("we already checked that the type is ColumnRef"),
                );
            }
            OptRelNodeTyp::Constant(_) => values.push(
                ConstantExpr::from_rel_node(uncasted_right)
                    .expect("we already checked that the type is Constant")
                    .value(),
            ),
            _ => {
                non_col_ref_exprs.push(uncasted_right);
            }
        }

        assert!(col_ref_exprs.len() + values.len() + non_col_ref_exprs.len() == 2);
        (col_ref_exprs, values, non_col_ref_exprs, is_left_col_ref)
    }

    /// Comparison operators are the base case for recursion in get_filter_selectivity()
    fn get_comp_op_selectivity(
        &self,
        comp_bin_op_typ: BinOpType,
        left: OptRelNodeRef,
        right: OptRelNodeRef,
        schema: &Schema,
        column_refs: &BaseTableColumnRefs,
    ) -> f64 {
        assert!(comp_bin_op_typ.is_comparison());

        // I intentionally performed moves on left and right. This way, we don't accidentally use them after this block
        let (col_ref_exprs, values, non_col_ref_exprs, is_left_col_ref) =
            Self::get_semantic_nodes(left, right, schema);

        // Handle the different cases of semantic nodes.
        if col_ref_exprs.is_empty() {
            UNIMPLEMENTED_SEL
        } else if col_ref_exprs.len() == 1 {
            let col_ref_expr = col_ref_exprs
                .first()
                .expect("we just checked that col_ref_exprs.len() == 1");
            let col_ref_idx = col_ref_expr.index();

            if let ColumnRef::BaseTableColumnRef(BaseTableColumnRef { table, col_idx }) =
                &column_refs[col_ref_idx]
            {
                if values.len() == 1 {
                    let value = values
                        .first()
                        .expect("we just checked that values.len() == 1");
                    match comp_bin_op_typ {
                        BinOpType::Eq => {
                            self.get_column_equality_selectivity(table, *col_idx, value, true)
                        }
                        BinOpType::Neq => {
                            self.get_column_equality_selectivity(table, *col_idx, value, false)
                        }
                        BinOpType::Lt | BinOpType::Leq | BinOpType::Gt | BinOpType::Geq => {
                            let start = match (comp_bin_op_typ, is_left_col_ref) {
                                (BinOpType::Lt, true) | (BinOpType::Geq, false) => Bound::Unbounded,
                                (BinOpType::Leq, true) | (BinOpType::Gt, false) => Bound::Unbounded,
                                (BinOpType::Gt, true) | (BinOpType::Leq, false) => Bound::Excluded(value),
                                (BinOpType::Geq, true) | (BinOpType::Lt, false) => Bound::Included(value),
                                _ => unreachable!("all comparison BinOpTypes were enumerated. this should be unreachable"),
                            };
                            let end = match (comp_bin_op_typ, is_left_col_ref) {
                                (BinOpType::Lt, true) | (BinOpType::Geq, false) => Bound::Excluded(value),
                                (BinOpType::Leq, true) | (BinOpType::Gt, false) => Bound::Included(value),
                                (BinOpType::Gt, true) | (BinOpType::Leq, false) => Bound::Unbounded,
                                (BinOpType::Geq, true) | (BinOpType::Lt, false) => Bound::Unbounded,
                                _ => unreachable!("all comparison BinOpTypes were enumerated. this should be unreachable"),
                            };
                            self.get_column_range_selectivity(table, *col_idx, start, end)
                        }
                        _ => unreachable!(
                            "all comparison BinOpTypes were enumerated. this should be unreachable"
                        ),
                    }
                } else {
                    let non_col_ref_expr = non_col_ref_exprs.first().expect(
                        "non_col_ref_exprs should have a value since col_ref_exprs.len() == 1",
                    );

                    match non_col_ref_expr.as_ref().typ {
                        OptRelNodeTyp::BinOp(_) => {
                            Self::get_default_comparison_op_selectivity(comp_bin_op_typ)
                        }
                        OptRelNodeTyp::Cast => UNIMPLEMENTED_SEL,
                        OptRelNodeTyp::Constant(_) => unreachable!(
                            "we should have handled this in the values.len() == 1 branch"
                        ),
                        _ => unimplemented!(
                            "unhandled case of comparing a column ref node to {}",
                            non_col_ref_expr.as_ref().typ
                        ),
                    }
                }
            } else {
                Self::get_default_comparison_op_selectivity(comp_bin_op_typ)
            }
        } else if col_ref_exprs.len() == 2 {
            Self::get_default_comparison_op_selectivity(comp_bin_op_typ)
        } else {
            unreachable!("we could have at most pushed left and right into col_ref_exprs")
        }
    }

    /// Get the selectivity of an expression of the form "column equals value" (or "value equals column")
    /// Will handle the case of statistics missing
    /// Equality predicates are handled entirely differently from range predicates so this is its own function
    /// Also, get_column_equality_selectivity is a subroutine when computing range selectivity, which is another
    ///     reason for separating these into two functions
    /// is_eq means whether it's == or !=
    fn get_column_equality_selectivity(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
        is_eq: bool,
    ) -> f64 {
        let ret_sel = if let Some(column_stats) = self.get_column_comb_stats(table, &[col_idx]) {
            let eq_freq = if let Some(freq) = column_stats.mcvs.freq(&vec![Some(value.clone())]) {
                freq
            } else {
                let non_mcv_freq = 1.0 - column_stats.mcvs.total_freq();
                // always safe because usize is at least as large as i32
                let ndistinct_as_usize = column_stats.ndistinct as usize;
                let non_mcv_cnt = ndistinct_as_usize - column_stats.mcvs.cnt();
                if non_mcv_cnt == 0 {
                    return 0.0;
                }
                // note that nulls are not included in ndistinct so we don't need to do non_mcv_cnt - 1 if null_frac > 0
                (non_mcv_freq - column_stats.null_frac) / (non_mcv_cnt as f64)
            };
            if is_eq {
                eq_freq
            } else {
                1.0 - eq_freq - column_stats.null_frac
            }
        } else {
            #[allow(clippy::collapsible_else_if)]
            if is_eq {
                DEFAULT_EQ_SEL
            } else {
                1.0 - DEFAULT_EQ_SEL
            }
        };
        assert!(
            (0.0..=1.0).contains(&ret_sel),
            "ret_sel ({}) should be in [0, 1]",
            ret_sel
        );
        ret_sel
    }

    /// Compute the frequency of values in a column less than or equal to the given value.
    fn get_column_leq_value_freq(
        per_column_stats: &ColumnCombValueStats<M, D>,
        value: &Value,
    ) -> f64 {
        // because distr does not include the values in MCVs, we need to compute the CDFs there as well
        // because nulls return false in any comparison, they are never included when computing range selectivity
        let distr_leq_freq = per_column_stats.distr.as_ref().unwrap().cdf(value);
        let value = value.clone();
        let pred = Box::new(move |val: &ColumnCombValue| *val[0].as_ref().unwrap() <= value);
        let mcvs_leq_freq = per_column_stats.mcvs.freq_over_pred(pred);
        let ret_freq = distr_leq_freq + mcvs_leq_freq;
        assert!(
            (0.0..=1.0).contains(&ret_freq),
            "ret_freq ({}) should be in [0, 1]",
            ret_freq
        );
        ret_freq
    }

    /// Compute the frequency of values in a column less than the given value.
    fn get_column_lt_value_freq(
        &self,
        column_stats: &ColumnCombValueStats<M, D>,
        table: &str,
        col_idx: usize,
        value: &Value,
    ) -> f64 {
        // depending on whether value is in mcvs or not, we use different logic to turn total_lt_cdf into total_leq_cdf
        // this logic just so happens to be the exact same logic as get_column_equality_selectivity implements
        let ret_freq = Self::get_column_leq_value_freq(column_stats, value)
            - self.get_column_equality_selectivity(table, col_idx, value, true);
        assert!(
            (0.0..=1.0).contains(&ret_freq),
            "ret_freq ({}) should be in [0, 1]",
            ret_freq
        );
        ret_freq
    }

    /// Get the selectivity of an expression of the form "column </<=/>=/> value" (or "value </<=/>=/> column").
    /// Computes selectivity based off of statistics.
    /// Range predicates are handled entirely differently from equality predicates so this is its own function.
    /// If it is unable to find the statistics, it returns DEFAULT_INEQ_SEL.
    /// The selectivity is computed as quantile of the right bound minus quantile of the left bound.
    fn get_column_range_selectivity(
        &self,
        table: &str,
        col_idx: usize,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> f64 {
        if let Some(column_stats) = self.get_column_comb_stats(table, &[col_idx]) {
            // Left and right quantile contain both Distribution and MCVs.
            let left_quantile = match start {
                Bound::Unbounded => 0.0,
                Bound::Included(value) => {
                    self.get_column_lt_value_freq(column_stats, table, col_idx, value)
                }
                Bound::Excluded(value) => Self::get_column_leq_value_freq(column_stats, value),
            };
            let right_quantile = match end {
                Bound::Unbounded => 1.0,
                Bound::Included(value) => Self::get_column_leq_value_freq(column_stats, value),
                Bound::Excluded(value) => {
                    self.get_column_lt_value_freq(column_stats, table, col_idx, value)
                }
            };
            assert!(
                left_quantile <= right_quantile,
                "left_quantile ({}) should be <= right_quantile ({})",
                left_quantile,
                right_quantile
            );
            right_quantile - left_quantile
        } else {
            DEFAULT_INEQ_SEL
        }
    }

    /// The default selectivity of a comparison expression
    /// Used when one side of the comparison is a column while the other side is something too
    ///   complex/impossible to evaluate (subquery, UDF, another column, we have no stats, etc.)
    fn get_default_comparison_op_selectivity(comp_bin_op_typ: BinOpType) -> f64 {
        assert!(comp_bin_op_typ.is_comparison());
        match comp_bin_op_typ {
            BinOpType::Eq => DEFAULT_EQ_SEL,
            BinOpType::Neq => 1.0 - DEFAULT_EQ_SEL,
            BinOpType::Lt | BinOpType::Leq | BinOpType::Gt | BinOpType::Geq => DEFAULT_INEQ_SEL,
            _ => unreachable!(
                "all comparison BinOpTypes were enumerated. this should be unreachable"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use optd_core::rel_node::Value;

    use crate::{
        cost::base_cost::{tests::*, DEFAULT_EQ_SEL},
        plan_nodes::{BinOpType, ConstantType, LogOpType, UnOpType},
        properties::{
            column_ref::ColumnRef,
            schema::{Field, Schema},
        },
    };

    #[test]
    fn test_const() {
        let cost_model = create_one_column_cost_model(get_empty_per_col_stats());
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(
                cnst(Value::Bool(true)),
                &Schema::new(vec![]),
                &vec![]
            ),
            1.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(
                cnst(Value::Bool(false)),
                &Schema::new(vec![]),
                &vec![]
            ),
            0.0
        );
    }

    #[test]
    fn test_colref_eq_constint_in_mcv() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1)));
        let expr_tree_rev = bin_op(BinOpType::Eq, cnst(Value::Int32(1)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.3
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.3
        );
    }

    #[test]
    fn test_colref_eq_constint_not_in_mcv() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.2), (Value::Int32(3), 0.44)]),
            5,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(2)));
        let expr_tree_rev = bin_op(BinOpType::Eq, cnst(Value::Int32(2)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.12
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.12
        );
    }

    /// I only have one test for NEQ since I'll assume that it uses the same underlying logic as EQ
    #[test]
    fn test_colref_neq_constint_in_mcv() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let expr_tree = bin_op(BinOpType::Neq, col_ref(0), cnst(Value::Int32(1)));
        let expr_tree_rev = bin_op(BinOpType::Neq, cnst(Value::Int32(1)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            1.0 - 0.3
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            1.0 - 0.3
        );
    }

    #[test]
    fn test_colref_leq_constint_no_mcvs_in_range() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            Some(TestDistribution::new(vec![(Value::Int32(15), 0.7)])),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.7
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.7
        );
    }

    #[test]
    fn test_colref_leq_constint_with_mcvs_in_range_not_at_border() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (vec![Some(Value::Int32(6))], 0.05),
                    (vec![Some(Value::Int32(10))], 0.1),
                    (vec![Some(Value::Int32(17))], 0.08),
                    (vec![Some(Value::Int32(25))], 0.07),
                ]
                .into_iter()
                .collect(),
            },
            10,
            0.0,
            Some(TestDistribution::new(vec![(Value::Int32(15), 0.7)])),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.85
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.85
        );
    }

    #[test]
    fn test_colref_leq_constint_with_mcv_at_border() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![
                (Value::Int32(6), 0.05),
                (Value::Int32(10), 0.1),
                (Value::Int32(15), 0.08),
                (Value::Int32(25), 0.07),
            ]),
            10,
            0.0,
            Some(TestDistribution::new(vec![(Value::Int32(15), 0.7)])),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.93
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.93
        );
    }

    #[test]
    fn test_colref_lt_constint_no_mcvs_in_range() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            Some(TestDistribution::new(vec![(Value::Int32(15), 0.7)])),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.6
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.6
        );
    }

    #[test]
    fn test_colref_lt_constint_with_mcvs_in_range_not_at_border() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (vec![Some(Value::Int32(6))], 0.05),
                    (vec![Some(Value::Int32(10))], 0.1),
                    (vec![Some(Value::Int32(17))], 0.08),
                    (vec![Some(Value::Int32(25))], 0.07),
                ]
                .into_iter()
                .collect(),
            },
            11, // there are 4 MCVs which together add up to 0.3. With 11 total ndistinct, each remaining value has freq 0.1
            0.0,
            Some(TestDistribution::new(vec![(Value::Int32(15), 0.7)])),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.75
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.75
        );
    }

    #[test]
    fn test_colref_lt_constint_with_mcv_at_border() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (vec![Some(Value::Int32(6))], 0.05),
                    (vec![Some(Value::Int32(10))], 0.1),
                    (vec![Some(Value::Int32(15))], 0.08),
                    (vec![Some(Value::Int32(25))], 0.07),
                ]
                .into_iter()
                .collect(),
            },
            11, // there are 4 MCVs which together add up to 0.3. With 11 total ndistinct, each remaining value has freq 0.1
            0.0,
            Some(TestDistribution::new(vec![(Value::Int32(15), 0.7)])),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.85
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.85
        );
    }

    /// I have fewer tests for GT since I'll assume that it uses the same underlying logic as LEQ
    /// The only interesting thing to test is that if there are nulls, those aren't included in GT
    #[test]
    fn test_colref_gt_constint() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            Some(TestDistribution::new(vec![(Value::Int32(15), 0.7)])),
        ));
        let expr_tree = bin_op(BinOpType::Gt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Leq, cnst(Value::Int32(15)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            1.0 - 0.7
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            1.0 - 0.7
        );
    }

    #[test]
    fn test_colref_geq_constint() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            Some(TestDistribution::new(vec![(Value::Int32(15), 0.7)])),
        ));
        let expr_tree = bin_op(BinOpType::Geq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Lt, cnst(Value::Int32(15)), col_ref(0));
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            1.0 - 0.6
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            1.0 - 0.6
        );
    }

    #[test]
    fn test_and() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (vec![Some(Value::Int32(1))], 0.3),
                    (vec![Some(Value::Int32(5))], 0.5),
                    (vec![Some(Value::Int32(8))], 0.2),
                ]
                .into_iter()
                .collect(),
            },
            0,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let eq1 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1)));
        let eq5 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(5)));
        let eq8 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(8)));
        let expr_tree = log_op(LogOpType::And, vec![eq1.clone(), eq5.clone(), eq8.clone()]);
        let expr_tree_shift1 = log_op(LogOpType::And, vec![eq5.clone(), eq8.clone(), eq1.clone()]);
        let expr_tree_shift2 = log_op(LogOpType::And, vec![eq8.clone(), eq1.clone(), eq5.clone()]);
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.03
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_shift1, &schema, &column_refs),
            0.03
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_shift2, &schema, &column_refs),
            0.03
        );
    }

    #[test]
    fn test_or() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (vec![Some(Value::Int32(1))], 0.3),
                    (vec![Some(Value::Int32(5))], 0.5),
                    (vec![Some(Value::Int32(8))], 0.2),
                ]
                .into_iter()
                .collect(),
            },
            0,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let eq1 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1)));
        let eq5 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(5)));
        let eq8 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(8)));
        let expr_tree = log_op(LogOpType::Or, vec![eq1.clone(), eq5.clone(), eq8.clone()]);
        let expr_tree_shift1 = log_op(LogOpType::Or, vec![eq5.clone(), eq8.clone(), eq1.clone()]);
        let expr_tree_shift2 = log_op(LogOpType::Or, vec![eq8.clone(), eq1.clone(), eq5.clone()]);
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.72
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_shift1, &schema, &column_refs),
            0.72
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_shift2, &schema, &column_refs),
            0.72
        );
    }

    #[test]
    fn test_not() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let expr_tree = un_op(
            UnOpType::Not,
            bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1))),
        );
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.7
        );
    }

    // I didn't test any non-unique cases with filter. The non-unique tests without filter should cover that

    #[test]
    fn test_colref_eq_cast_value() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.1,
            Some(TestDistribution::empty()),
        ));
        let expr_tree = bin_op(
            BinOpType::Eq,
            col_ref(0),
            cast(cnst(Value::Int64(1)), DataType::Int32),
        );
        let expr_tree_rev = bin_op(
            BinOpType::Eq,
            cast(cnst(Value::Int64(1)), DataType::Int32),
            col_ref(0),
        );
        let schema = Schema::new(vec![]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.3
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.3
        );
    }

    #[test]
    fn test_cast_colref_eq_value() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.1,
            Some(TestDistribution::empty()),
        ));
        let expr_tree = bin_op(
            BinOpType::Eq,
            cast(col_ref(0), DataType::Int64),
            cnst(Value::Int64(1)),
        );
        let expr_tree_rev = bin_op(
            BinOpType::Eq,
            cnst(Value::Int64(1)),
            cast(col_ref(0), DataType::Int64),
        );
        let schema = Schema::new(vec![Field {
            name: String::from(""),
            typ: ConstantType::Int32,
            nullable: false,
        }]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            0.3
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            0.3
        );
    }

    /// In this case, we should leave the Cast as is.
    ///
    /// Note that the test only checks the selectivity and thus doesn't explicitly test that the
    /// Cast is indeed left as is. However, if get_filter_selectivity() doesn't crash, that's a
    /// pretty good signal that the Cast was left as is.
    #[test]
    fn test_cast_colref_eq_colref() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![]),
            0,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let expr_tree = bin_op(BinOpType::Eq, cast(col_ref(0), DataType::Int64), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), cast(col_ref(0), DataType::Int64));
        let schema = Schema::new(vec![
            Field {
                name: String::from(""),
                typ: ConstantType::Int32,
                nullable: false,
            },
            Field {
                name: String::from(""),
                typ: ConstantType::Int64,
                nullable: false,
            },
        ]);
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &schema, &column_refs),
            DEFAULT_EQ_SEL
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &schema, &column_refs),
            DEFAULT_EQ_SEL
        );
    }
}
