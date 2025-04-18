// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use anyhow::{bail, Result};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{self, logical_plan, LogicalPlan, Operator};
use datafusion::scalar::ScalarValue;
use datafusion_expr::{ExprSchemable, Subquery};
use itertools::Itertools;
use optd_og_core::nodes::PredNode;
use optd_og_datafusion_repr::plan_nodes::{
    ArcDfPlanNode, ArcDfPredNode, BetweenPred, BinOpPred, BinOpType, CastPred, ColumnRefPred,
    ConstantPred, DfReprPlanNode, DfReprPredNode, ExternColumnRefPred, FuncPred, FuncType,
    InListPred, JoinType, LikePred, ListPred, LogOpPred, LogOpType, LogicalAgg,
    LogicalEmptyRelation, LogicalFilter, LogicalJoin, LogicalLimit, LogicalProjection, LogicalScan,
    LogicalSort, RawDependentJoin, SortOrderPred, SortOrderType, SubqueryType,
};
use optd_og_datafusion_repr::properties::schema::Schema as OptdSchema;

use crate::OptdPlanContext;

impl OptdPlanContext<'_> {
    fn subqueries_to_dependent_joins(
        &mut self,
        subqueries: Vec<(&Subquery, SubqueryType)>,
        input: ArcDfPlanNode,
        input_schema: &DFSchema,
    ) -> Result<ArcDfPlanNode> {
        let mut node = input;
        for (
            Subquery {
                subquery,
                outer_ref_columns,
            },
            sq_typ,
        ) in subqueries.into_iter()
        {
            let subquery_root = self.conv_into_optd_og_plan_node(subquery, Some(input_schema))?;
            let dep_join = RawDependentJoin::new(
                node,
                subquery_root,
                ConstantPred::bool(true).into_pred_node(),
                ListPred::new(
                    outer_ref_columns
                        .iter()
                        .filter_map(|col| {
                            if let datafusion_expr::Expr::OuterReferenceColumn(_, col) = col {
                                Some(
                                    ExternColumnRefPred::new(
                                        input_schema.index_of_column(col).unwrap(),
                                    )
                                    .into_pred_node(),
                                )
                            } else {
                                None
                            }
                        })
                        .collect(),
                ),
                sq_typ,
            );
            node = dep_join.into_plan_node();
        }
        Ok(node)
    }

    fn conv_into_optd_og_table_scan(
        &mut self,
        node: &logical_plan::TableScan,
    ) -> Result<ArcDfPlanNode> {
        let table_name = node.table_name.to_string();
        if node.fetch.is_some() {
            bail!("fetch")
        }
        if !node.filters.is_empty() {
            bail!("no filters")
        }
        self.tables.insert(table_name.clone(), node.source.clone());
        let scan = LogicalScan::new(table_name);
        if let Some(ref projection) = node.projection {
            let mut exprs = Vec::with_capacity(projection.len());
            for &p in projection {
                exprs.push(ColumnRefPred::new(p).into_pred_node());
            }
            let projection = LogicalProjection::new(scan.into_plan_node(), ListPred::new(exprs));
            return Ok(projection.into_plan_node());
        }
        Ok(scan.into_plan_node())
    }

    fn conv_into_optd_og_expr<'a>(
        &mut self,
        expr: &'a logical_expr::Expr,
        context: &DFSchema,
        dep_ctx: Option<&DFSchema>,
        subqueries: &mut Vec<(&'a Subquery, SubqueryType)>,
    ) -> Result<ArcDfPredNode> {
        use logical_expr::Expr;
        match expr {
            Expr::BinaryExpr(node) => {
                let left =
                    self.conv_into_optd_og_expr(node.left.as_ref(), context, dep_ctx, subqueries)?;
                let right =
                    self.conv_into_optd_og_expr(node.right.as_ref(), context, dep_ctx, subqueries)?;
                match node.op {
                    Operator::And => {
                        let op = LogOpType::And;
                        let expr_list = ListPred::new(vec![left, right]);
                        return Ok(
                            LogOpPred::new_flattened_nested_logical(op, expr_list).into_pred_node()
                        );
                    }
                    Operator::Or => {
                        let op = LogOpType::Or;
                        let expr_list = ListPred::new(vec![left, right]);
                        return Ok(
                            LogOpPred::new_flattened_nested_logical(op, expr_list).into_pred_node()
                        );
                    }
                    _ => {}
                }

                let op = match node.op {
                    Operator::Eq => BinOpType::Eq,
                    Operator::NotEq => BinOpType::Neq,
                    Operator::LtEq => BinOpType::Leq,
                    Operator::Lt => BinOpType::Lt,
                    Operator::GtEq => BinOpType::Geq,
                    Operator::Gt => BinOpType::Gt,
                    Operator::Plus => BinOpType::Add,
                    Operator::Minus => BinOpType::Sub,
                    Operator::Multiply => BinOpType::Mul,
                    Operator::Divide => BinOpType::Div,
                    op => unimplemented!("{}", op),
                };
                Ok(BinOpPred::new(left, right, op).into_pred_node())
            }
            Expr::Column(col) => {
                let idx = context.index_of_column(col)?;
                Ok(ColumnRefPred::new(idx).into_pred_node())
            }
            Expr::OuterReferenceColumn(_, col) => {
                let idx = dep_ctx.unwrap().index_of_column(col)?;
                Ok(ExternColumnRefPred::new(idx).into_pred_node())
            }
            Expr::Literal(x) => match x {
                ScalarValue::UInt8(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::uint8(*x).into_pred_node())
                }
                ScalarValue::UInt16(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::uint16(*x).into_pred_node())
                }
                ScalarValue::UInt32(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::uint32(*x).into_pred_node())
                }
                ScalarValue::UInt64(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::uint64(*x).into_pred_node())
                }
                ScalarValue::Int8(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::int8(*x).into_pred_node())
                }
                ScalarValue::Int16(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::int16(*x).into_pred_node())
                }
                ScalarValue::Int32(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::int32(*x).into_pred_node())
                }
                ScalarValue::Int64(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::int64(*x).into_pred_node())
                }
                ScalarValue::Float64(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::float64(*x).into_pred_node())
                }
                ScalarValue::Utf8(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::string(x).into_pred_node())
                }
                ScalarValue::Date32(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::date(*x as i64).into_pred_node())
                }
                ScalarValue::IntervalMonthDayNano(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::interval_month_day_nano(
                        ((((x.months as i128) << 32) + x.days as i128) << 64)
                            + x.nanoseconds as i128,
                    )
                    .into_pred_node())
                }
                ScalarValue::Decimal128(x, _, _) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::decimal(*x as f64).into_pred_node())
                }
                ScalarValue::Boolean(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantPred::bool(*x).into_pred_node())
                }
                _ => bail!("{:?}", x),
            },
            Expr::Alias(x) => {
                self.conv_into_optd_og_expr(x.expr.as_ref(), context, dep_ctx, subqueries)
            }
            Expr::ScalarFunction(x) => {
                let args = self.conv_into_optd_og_expr_list(&x.args, context, dep_ctx, subqueries)?;
                Ok(FuncPred::new(
                    FuncType::new_scalar(
                        x.func.name().to_string(),
                        // TODO: remove this infer...
                        x.func
                            .return_type_from_exprs(
                                &x.args,
                                context,
                                &x.args
                                    .iter()
                                    .map(|x| x.get_type(context).unwrap())
                                    .collect_vec(),
                            )
                            .unwrap(),
                    ),
                    args,
                )
                .into_pred_node())
            }
            Expr::AggregateFunction(x) => {
                let args = self.conv_into_optd_og_expr_list(&x.params.args, context, dep_ctx, subqueries)?;
                Ok(
                    FuncPred::new(FuncType::new_agg(x.func.name().to_string()), args)
                        .into_pred_node(),
                )
            }
            Expr::Case(x) => {
                let when_then_expr = &x.when_then_expr;
                assert_eq!(when_then_expr.len(), 1);
                let (when_expr, then_expr) = &when_then_expr[0];
                let when_expr =
                    self.conv_into_optd_og_expr(when_expr, context, dep_ctx, subqueries)?;
                let then_expr =
                    self.conv_into_optd_og_expr(then_expr, context, dep_ctx, subqueries)?;
                let else_expr = self.conv_into_optd_og_expr(
                    x.else_expr.as_ref().unwrap(),
                    context,
                    dep_ctx,
                    subqueries,
                )?;
                assert!(x.expr.is_none());
                Ok(FuncPred::new(
                    FuncType::Case,
                    ListPred::new(vec![when_expr, then_expr, else_expr]),
                )
                .into_pred_node())
            }
            Expr::Not(x) => {
                let expr = self.conv_into_optd_og_expr(x.as_ref(), context, dep_ctx, subqueries)?;
                Ok(FuncPred::new(FuncType::Not, ListPred::new(vec![expr])).into_pred_node())
            }
            Expr::IsNull(x) => {
                let expr = self.conv_into_optd_og_expr(x.as_ref(), context, dep_ctx, subqueries)?;
                Ok(FuncPred::new(FuncType::IsNull, ListPred::new(vec![expr])).into_pred_node())
            }
            Expr::IsNotNull(x) => {
                let expr = self.conv_into_optd_og_expr(x.as_ref(), context, dep_ctx, subqueries)?;
                Ok(FuncPred::new(FuncType::IsNotNull, ListPred::new(vec![expr])).into_pred_node())
            }
            Expr::Between(x) => {
                let expr =
                    self.conv_into_optd_og_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
                let low = self.conv_into_optd_og_expr(x.low.as_ref(), context, dep_ctx, subqueries)?;
                let high =
                    self.conv_into_optd_og_expr(x.high.as_ref(), context, dep_ctx, subqueries)?;
                assert!(!x.negated, "unimplemented");
                Ok(BetweenPred::new(expr, low, high).into_pred_node())
            }
            Expr::Cast(x) => {
                let expr =
                    self.conv_into_optd_og_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
                Ok(CastPred::new(expr, x.data_type.clone()).into_pred_node())
            }
            Expr::Like(x) => {
                let expr =
                    self.conv_into_optd_og_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
                let pattern =
                    self.conv_into_optd_og_expr(x.pattern.as_ref(), context, dep_ctx, subqueries)?;
                Ok(LikePred::new(x.negated, x.case_insensitive, expr, pattern).into_pred_node())
            }
            Expr::InList(x) => {
                let expr =
                    self.conv_into_optd_og_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
                let list = self.conv_into_optd_og_expr_list(&x.list, context, dep_ctx, subqueries)?;
                Ok(InListPred::new(expr, list, x.negated).into_pred_node())
            }
            Expr::ScalarSubquery(sq) => {
                // This relies on a left-deep tree of dependent joins being
                // generated below this node, in response to all pushed subqueries.
                let new_column_ref_idx = context.fields().len() + subqueries.len();
                subqueries.push((sq, SubqueryType::Scalar));
                Ok(ColumnRefPred::new(new_column_ref_idx).into_pred_node())
            }
            Expr::Exists(ex) => {
                let sq = &ex.subquery;
                let negated = ex.negated;

                let new_column_ref_idx = context.fields().len() + subqueries.len();
                subqueries.push((sq, SubqueryType::Exists));
                if negated {
                    Ok(FuncPred::new(
                        FuncType::Not,
                        ListPred::new(
                            vec![ColumnRefPred::new(new_column_ref_idx).into_pred_node()],
                        ),
                    )
                    .into_pred_node())
                } else {
                    Ok(ColumnRefPred::new(new_column_ref_idx).into_pred_node())
                }
            }
            Expr::InSubquery(insq) => {
                let sq = &insq.subquery;
                let expr =
                    self.conv_into_optd_og_expr(insq.expr.as_ref(), context, dep_ctx, subqueries)?;
                let negated = insq.negated;

                let new_column_ref_idx = context.fields().len() + subqueries.len();
                subqueries.push((
                    sq,
                    SubqueryType::Any {
                        pred: Arc::unwrap_or_clone(expr),
                        op: BinOpType::Eq,
                    },
                ));
                if negated {
                    Ok(FuncPred::new(
                        FuncType::Not,
                        ListPred::new(
                            vec![ColumnRefPred::new(new_column_ref_idx).into_pred_node()],
                        ),
                    )
                    .into_pred_node())
                } else {
                    Ok(ColumnRefPred::new(new_column_ref_idx).into_pred_node())
                }
            }
            _ => bail!("Unsupported expression: {:?}", expr),
        }
    }

    fn conv_into_optd_og_projection(
        &mut self,
        node: &logical_plan::Projection,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalProjection> {
        let input = self.conv_into_optd_og_plan_node(node.input.as_ref(), dep_ctx)?;
        let mut subqueries = vec![];
        let expr_list = self.conv_into_optd_og_expr_list(
            &node.expr,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;
        let input = self.subqueries_to_dependent_joins(subqueries, input, node.input.schema())?;
        Ok(LogicalProjection::new(input, expr_list))
    }

    fn conv_into_optd_og_filter(
        &mut self,
        node: &logical_plan::Filter,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalFilter> {
        let input = self.conv_into_optd_og_plan_node(node.input.as_ref(), dep_ctx)?;
        let mut subqueries = vec![];
        let expr = self.conv_into_optd_og_expr(
            &node.predicate,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;

        let input = self.subqueries_to_dependent_joins(subqueries, input, node.input.schema())?;

        Ok(LogicalFilter::new(input, expr))
    }

    fn conv_into_optd_og_expr_list<'a>(
        &mut self,
        exprs: &'a [logical_expr::Expr],
        context: &DFSchema,
        dep_ctx: Option<&DFSchema>,
        subqueries: &mut Vec<(&'a Subquery, SubqueryType)>,
    ) -> Result<ListPred> {
        let exprs = exprs
            .iter()
            .map(|expr| self.conv_into_optd_og_expr(expr, context, dep_ctx, subqueries))
            .collect::<Result<Vec<_>>>()?;
        Ok(ListPred::new(exprs))
    }

    fn conv_into_optd_og_expr_sort_list<'a>(
        &mut self,
        exprs: &'a [logical_expr::SortExpr],
        context: &DFSchema,
        dep_ctx: Option<&DFSchema>,
        subqueries: &mut Vec<(&'a Subquery, SubqueryType)>,
    ) -> Result<ListPred> {
        let exprs = exprs
            .iter()
            .map(|sort| {
                Ok(SortOrderPred::new(
                    if sort.asc {
                        SortOrderType::Asc
                    } else {
                        SortOrderType::Desc
                    },
                    // TODO: null_first
                    self.conv_into_optd_og_expr(&sort.expr, context, dep_ctx, subqueries)?,
                )
                .into_pred_node())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ListPred::new(exprs))
    }

    fn conv_into_optd_og_sort(
        &mut self,
        node: &logical_plan::Sort,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalSort> {
        let input = self.conv_into_optd_og_plan_node(node.input.as_ref(), dep_ctx)?;
        let mut subqueries = vec![];
        let expr_list = self.conv_into_optd_og_expr_sort_list(
            &node.expr,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;
        assert!(
            subqueries.is_empty(),
            "Subqueries encountered in conv_into_optd_og_sort---not supported currently"
        );
        Ok(LogicalSort::new(input, expr_list))
    }

    fn conv_into_optd_og_agg(
        &mut self,
        node: &logical_plan::Aggregate,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalAgg> {
        let input = self.conv_into_optd_og_plan_node(node.input.as_ref(), dep_ctx)?;
        let mut subqueries = vec![];
        let agg_exprs = self.conv_into_optd_og_expr_list(
            &node.aggr_expr,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;
        let group_exprs = self.conv_into_optd_og_expr_list(
            &node.group_expr,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;
        assert!(
            subqueries.is_empty(),
            "Subqueries encountered in conv_into_optd_og_agg---not supported currently"
        );
        Ok(LogicalAgg::new(input, agg_exprs, group_exprs))
    }

    fn add_column_offset(offset: usize, expr: ArcDfPredNode) -> ArcDfPredNode {
        if let Some(expr) = ColumnRefPred::from_pred_node(expr.clone()) {
            return ColumnRefPred::new(expr.index() + offset).into_pred_node();
        }
        let children = expr
            .children
            .iter()
            .map(|child| {
                let child = child.clone();
                Self::add_column_offset(offset, child)
            })
            .collect();
        PredNode {
            typ: expr.typ.clone(),
            children,
            data: expr.data.clone(),
        }
        .into()
    }

    fn conv_into_optd_og_join(
        &mut self,
        node: &logical_plan::Join,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalJoin> {
        use logical_plan::JoinType as DFJoinType;
        let left = self.conv_into_optd_og_plan_node(node.left.as_ref(), dep_ctx)?;
        let right = self.conv_into_optd_og_plan_node(node.right.as_ref(), dep_ctx)?;
        let join_type = match node.join_type {
            DFJoinType::Inner => JoinType::Inner,
            DFJoinType::Left => JoinType::LeftOuter,
            DFJoinType::Right => JoinType::RightOuter,
            DFJoinType::Full => JoinType::FullOuter,
            DFJoinType::LeftAnti => JoinType::LeftAnti,
            DFJoinType::RightAnti => JoinType::RightAnti,
            DFJoinType::LeftSemi => JoinType::LeftSemi,
            DFJoinType::RightSemi => JoinType::RightSemi,
            DFJoinType::LeftMark => JoinType::LeftMark,
        };
        let mut log_ops = Vec::with_capacity(node.on.len());
        let mut subqueries = vec![];
        for (left, right) in &node.on {
            let left =
                self.conv_into_optd_og_expr(left, node.left.schema(), dep_ctx, &mut subqueries)?;
            let right =
                self.conv_into_optd_og_expr(right, node.right.schema(), dep_ctx, &mut subqueries)?;
            let right = Self::add_column_offset(node.left.schema().fields().len(), right);
            let op = BinOpType::Eq;
            let expr = BinOpPred::new(left, right, op).into_pred_node();
            log_ops.push(expr);
        }
        if node.filter.is_some() {
            let filter = self.conv_into_optd_og_expr(
                node.filter.as_ref().unwrap(),
                node.schema.as_ref(),
                dep_ctx,
                &mut subqueries,
            )?;
            log_ops.push(filter);
        }
        assert!(
            subqueries.is_empty(),
            "Subqueries encountered in conv_into_optd_og_join---not supported currently"
        );

        if log_ops.is_empty() {
            Ok(LogicalJoin::new(
                left,
                right,
                ConstantPred::bool(true).into_pred_node(),
                JoinType::Inner,
            ))
        } else if log_ops.len() == 1 {
            Ok(LogicalJoin::new(left, right, log_ops.remove(0), join_type))
        } else {
            let expr_list = ListPred::new(log_ops);
            // the expr from filter is already flattened in conv_into_optd_og_expr
            let log_op = LogOpPred::new_flattened_nested_logical(LogOpType::And, expr_list);
            Ok(LogicalJoin::new(
                left,
                right,
                log_op.into_pred_node(),
                join_type,
            ))
        }
    }

    fn conv_into_optd_og_empty_relation(
        &mut self,
        node: &logical_plan::EmptyRelation,
    ) -> Result<LogicalEmptyRelation> {
        // empty_relation from datafusion always have an empty schema
        let empty_schema = OptdSchema { fields: vec![] };
        Ok(LogicalEmptyRelation::new(
            node.produce_one_row,
            empty_schema,
        ))
    }

    fn conv_into_optd_og_limit(
        &mut self,
        node: &logical_plan::Limit,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalLimit> {
        let input = self.conv_into_optd_og_plan_node(node.input.as_ref(), dep_ctx)?;
        let skip = node
            .skip
            .as_ref()
            .map(|expr| {
                self.conv_into_optd_og_expr(expr.as_ref(), node.input.schema(), dep_ctx, &mut vec![])
            })
            .transpose()?
            .unwrap_or_else(|| ConstantPred::int64(0).into_pred_node());
        let fetch = node
            .fetch
            .as_ref()
            .map(|expr| {
                self.conv_into_optd_og_expr(expr.as_ref(), node.input.schema(), dep_ctx, &mut vec![])
            })
            .transpose()?
            .unwrap_or_else(|| ConstantPred::int64(i64::MAX).into_pred_node());
        Ok(LogicalLimit::new(input, skip, fetch))
    }

    fn conv_into_optd_og_plan_node(
        &mut self,
        node: &LogicalPlan,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<ArcDfPlanNode> {
        let node = match node {
            LogicalPlan::TableScan(node) => self.conv_into_optd_og_table_scan(node)?.into_plan_node(),
            LogicalPlan::Projection(node) => self
                .conv_into_optd_og_projection(node, dep_ctx)?
                .into_plan_node(),
            LogicalPlan::Sort(node) => self.conv_into_optd_og_sort(node, dep_ctx)?.into_plan_node(),
            LogicalPlan::Aggregate(node) => {
                self.conv_into_optd_og_agg(node, dep_ctx)?.into_plan_node()
            }
            LogicalPlan::SubqueryAlias(node) => {
                self.conv_into_optd_og_plan_node(node.input.as_ref(), dep_ctx)?
            }
            LogicalPlan::Join(node) => self.conv_into_optd_og_join(node, dep_ctx)?.into_plan_node(),
            LogicalPlan::Filter(node) => {
                self.conv_into_optd_og_filter(node, dep_ctx)?.into_plan_node()
            }
            LogicalPlan::EmptyRelation(node) => {
                self.conv_into_optd_og_empty_relation(node)?.into_plan_node()
            }
            LogicalPlan::Limit(node) => self.conv_into_optd_og_limit(node, dep_ctx)?.into_plan_node(),
            _ => bail!(
                "unsupported plan node: {}",
                format!("{:?}", node).split('\n').next().unwrap()
            ),
        };
        Ok(node)
    }

    pub fn conv_into_optd_og(&mut self, root_rel: &LogicalPlan) -> Result<ArcDfPlanNode> {
        let res = self.conv_into_optd_og_plan_node(root_rel, None)?;
        Ok(res.into_plan_node())
    }
}
