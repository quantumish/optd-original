use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{cost::OptCostModel, plan_nodes::OptRelNodeTyp};
use optd_core::{
    cascades::{CascadesOptimizer, GroupId, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, Value},
};

use super::base_cost::DEFAULT_TABLE_ROW_CNT;

pub type RuntimeAdaptionStorage = Arc<Mutex<RuntimeAdaptionStorageInner>>;

#[derive(Default, Debug)]
pub struct RuntimeAdaptionStorageInner {
    pub history: HashMap<GroupId, (usize, usize)>,
    pub iter_cnt: usize,
}

pub struct AdaptiveCostModel {
    runtime_row_cnt: RuntimeAdaptionStorage,
    base_model: OptCostModel,
    decay: usize,
}

impl CostModel<OptRelNodeTyp> for AdaptiveCostModel {
    fn explain(&self, cost: &Cost) -> String {
        self.base_model.explain(cost)
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {
        self.base_model.accumulate(total_cost, cost)
    }

    fn zero(&self) -> Cost {
        self.base_model.zero()
    }

    fn compute_cost(
        &self,
        node: &OptRelNodeTyp,
        data: &Option<Value>,
        children: &[Cost],
        context: Option<RelNodeContext>,
        _optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        if let OptRelNodeTyp::PhysicalScan = node {
            let guard = self.runtime_row_cnt.lock().unwrap();
            if let Some((runtime_row_cnt, iter)) = guard.history.get(&context.unwrap().group_id) {
                if *iter + self.decay >= guard.iter_cnt {
                    let runtime_row_cnt = (*runtime_row_cnt).max(1) as f64;
                    return OptCostModel::cost(runtime_row_cnt, 0.0, runtime_row_cnt);
                } else {
                    return OptCostModel::cost(DEFAULT_TABLE_ROW_CNT as f64, 0.0, 1.0);
                }
            } else {
                return OptCostModel::cost(DEFAULT_TABLE_ROW_CNT as f64, 0.0, 1.0);
            }
        }
        let (mut row_cnt, compute_cost, io_cost) = OptCostModel::cost_tuple(
            &self
                .base_model
                .compute_cost(node, data, children, None, None),
        );
        if let Some(context) = context {
            let guard = self.runtime_row_cnt.lock().unwrap();
            if let Some((runtime_row_cnt, iter)) = guard.history.get(&context.group_id) {
                if *iter + self.decay >= guard.iter_cnt {
                    let runtime_row_cnt = (*runtime_row_cnt).max(1) as f64;
                    row_cnt = runtime_row_cnt;
                }
            }
        }
        OptCostModel::cost(row_cnt, compute_cost, io_cost)
    }

    fn compute_plan_node_cost(&self, node: &RelNode<OptRelNodeTyp>) -> Cost {
        self.base_model.compute_plan_node_cost(node)
    }
}

impl AdaptiveCostModel {
    pub fn new(decay: usize) -> Self {
        Self {
            runtime_row_cnt: Arc::new(Mutex::new(RuntimeAdaptionStorageInner::default())),
            base_model: OptCostModel::new(HashMap::new()),
            decay,
        }
    }

    pub fn get_runtime_map(&self) -> RuntimeAdaptionStorage {
        self.runtime_row_cnt.clone()
    }
}
