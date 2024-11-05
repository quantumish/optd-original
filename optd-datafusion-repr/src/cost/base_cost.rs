use std::collections::HashMap;

use crate::plan_nodes::{ArcDfPredNode, ConstantPred, DfNodeType};
use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, NaiveMemo, RelNodeContext},
    cost::{Cost, CostModel, Statistics},
    nodes::{ArcPredNode, Value},
};
use value_bag::ValueBag;

pub struct DfCostModel {
    table_stat: HashMap<String, usize>,
}

pub const COMPUTE_COST: usize = 0;
pub const IO_COST: usize = 1;

pub(crate) const DEFAULT_TABLE_ROW_CNT: usize = 1000;

impl DfCostModel {
    pub fn compute_cost(Cost(cost): &Cost) -> f64 {
        cost[COMPUTE_COST]
    }

    pub fn io_cost(Cost(cost): &Cost) -> f64 {
        cost[IO_COST]
    }

    pub fn row_cnt(Statistics(stat): &Statistics) -> f64 {
        stat.by_ref().as_f64()
    }

    pub fn cost(compute_cost: f64, io_cost: f64) -> Cost {
        Cost(vec![compute_cost, io_cost])
    }

    pub fn stat(row_cnt: f64) -> Statistics {
        Statistics(ValueBag::from_f64(row_cnt).to_owned())
    }

    pub fn cost_tuple(Cost(cost): &Cost) -> (f64, f64) {
        (cost[COMPUTE_COST], cost[IO_COST])
    }

    fn get_row_cnt(&self, table_name: &str) -> f64 {
        self.table_stat
            .get(table_name.as_ref())
            .copied()
            .unwrap_or(DEFAULT_TABLE_ROW_CNT) as f64
    }
}

impl CostModel<DfNodeType, NaiveMemo<DfNodeType>> for DfCostModel {
    fn explain_cost(&self, cost: &Cost) -> String {
        format!(
            "{{compute={},io={}}}",
            Self::compute_cost(cost),
            Self::io_cost(cost)
        )
    }

    fn explain_statistics(&self, stat: &Statistics) -> String {
        format!("{{row_cnt={}}}", Self::row_cnt(stat))
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {
        total_cost.0[COMPUTE_COST] += Self::compute_cost(cost);
        total_cost.0[IO_COST] += Self::io_cost(cost);
    }

    fn zero(&self) -> Cost {
        Cost(vec![0.0, 0.0])
    }

    fn derive_statistics(
        &self,
        node: &DfNodeType,
        predicates: &[ArcPredNode<DfNodeType>],
        children: &[&Statistics],
        _context: Option<RelNodeContext>,
        _optimizer: Option<&CascadesOptimizer<DfNodeType, NaiveMemo<DfNodeType>>>,
    ) -> Statistics {
        match node {
            DfNodeType::PhysicalScan => {
                let table_name = ConstantPred::from_pred_node(predicates[0])
                    .unwrap()
                    .as_str();
                let row_cnt = self.get_row_cnt(table_name);
                Self::stat(row_cnt)
            }
            DfNodeType::PhysicalLimit => {
                let row_cnt = Self::row_cnt(children[0]);
                let selectivity = 0.001;
                Self::stat((row_cnt * selectivity).max(1.0))
            }
            DfNodeType::PhysicalEmptyRelation => Self::stat(0.01),
            DfNodeType::PhysicalFilter => {
                let row_cnt = Self::row_cnt(children[0]);
                let selectivity = 0.001;
                Self::stat((row_cnt * selectivity).max(1.0))
            }
            DfNodeType::PhysicalNestedLoopJoin(_) => {
                let row_cnt_1 = Self::row_cnt(children[0]);
                let row_cnt_2 = Self::row_cnt(children[1]);
                let selectivity = 0.01;
                Self::stat((row_cnt_1 * row_cnt_2 * selectivity).max(1.0))
            }
            DfNodeType::PhysicalHashJoin(_) => {
                let row_cnt_1 = Self::row_cnt(children[0]);
                let row_cnt_2 = Self::row_cnt(children[1]);
                Self::stat(row_cnt_1.min(row_cnt_2).max(1.0))
            }
            DfNodeType::PhysicalSort | DfNodeType::PhysicalAgg | DfNodeType::PhysicalProjection => {
                let row_cnt = Self::row_cnt(children[0]);
                Self::stat(row_cnt)
            }
            DfNodeType::List => Self::stat(1.0),
            _ if node.is_expression() => Self::stat(1.0),
            x => unimplemented!("cannot derive statistics for {}", x),
        }
    }

    fn compute_operation_cost(
        &self,
        node: &DfNodeType,
        children: &[Option<&Statistics>],
        predicates: &[ArcDfPredNode],
        children_cost: &[Cost],
        _context: Option<RelNodeContext>,
        _optimizer: Option<&CascadesOptimizer<DfNodeType, NaiveMemo<DfNodeType>>>,
    ) -> Cost {
        let row_cnts = children
            .iter()
            .map(|child| child.map(Self::row_cnt).unwrap_or(0 as f64))
            .collect_vec();
        match node {
            DfNodeType::PhysicalScan => {
                let table_name = ConstantPred::from_pred_node(predicates[0])
                    .unwrap()
                    .as_str();
                let row_cnt = self.get_row_cnt(table_name);
                Self::cost(0.0, row_cnt)
            }
            DfNodeType::PhysicalLimit => {
                let row_cnt = row_cnts[0];
                Self::cost(row_cnt, 0.0)
            }
            DfNodeType::PhysicalEmptyRelation => Self::cost(0.01, 0.0),
            DfNodeType::PhysicalFilter => {
                let row_cnt = row_cnts[0];
                let (compute_cost, _) = Self::cost_tuple(&children_cost[1]);
                Self::cost(row_cnt * compute_cost, 0.0)
            }
            DfNodeType::PhysicalNestedLoopJoin(_) => {
                let row_cnt_1 = row_cnts[0];
                let row_cnt_2 = row_cnts[1];
                let (compute_cost, _) = Self::cost_tuple(&children_cost[2]);
                Self::cost(row_cnt_1 * row_cnt_2 * compute_cost + row_cnt_1, 0.0)
            }
            DfNodeType::PhysicalProjection => {
                let row_cnt = row_cnts[0];
                let (compute_cost, _) = Self::cost_tuple(&children_cost[1]);
                Self::cost(row_cnt * compute_cost, 0.0)
            }
            DfNodeType::PhysicalHashJoin(_) => {
                let row_cnt_1 = row_cnts[0];
                let row_cnt_2 = row_cnts[1];
                Self::cost(row_cnt_1 * 2.0 + row_cnt_2, 0.0)
            }
            DfNodeType::PhysicalSort => {
                let row_cnt = row_cnts[0];
                Self::cost(row_cnt * row_cnt.ln_1p().max(1.0), 0.0)
            }
            DfNodeType::PhysicalAgg => {
                let row_cnt = row_cnts[0];
                let (compute_cost_1, _) = Self::cost_tuple(&children_cost[1]);
                let (compute_cost_2, _) = Self::cost_tuple(&children_cost[2]);
                Self::cost(row_cnt * (compute_cost_1 + compute_cost_2), 0.0)
            }
            // List and expressions are computed in the same way -- but list has much fewer cost
            DfNodeType::List => {
                let compute_cost = children_cost
                    .iter()
                    .map(|child| {
                        let (compute_cost, _) = Self::cost_tuple(child);
                        compute_cost
                    })
                    .sum::<f64>();
                Self::cost(compute_cost + 0.01, 0.0)
            }
            _ if node.is_expression() => {
                let compute_cost = children_cost
                    .iter()
                    .map(|child| {
                        let (compute_cost, _) = Self::cost_tuple(child);
                        compute_cost
                    })
                    .sum::<f64>();
                Self::cost(compute_cost + 1.0, 0.0)
            }
            x => unimplemented!("cannot compute cost for {}", x),
        }
    }

    fn weighted_cost(&self, cost: &Cost) -> f64 {
        Self::compute_cost(cost) + Self::io_cost(cost)
    }
}

impl DfCostModel {
    pub fn new(table_stat: HashMap<String, usize>) -> Self {
        Self { table_stat }
    }
}
