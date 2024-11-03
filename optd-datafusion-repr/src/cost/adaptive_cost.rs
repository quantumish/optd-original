use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    cost::DfCostModel,
    plan_nodes::{ArcDfPredNode, DfNodeType},
};
use optd_core::{
    cascades::{CascadesOptimizer, GroupId, RelNodeContext},
    cost::{Cost, CostModel},
    nodes::{PlanNode, PredNode, Value},
};

pub type RuntimeAdaptionStorage = Arc<Mutex<RuntimeAdaptionStorageInner>>;

#[derive(Default, Debug)]
pub struct RuntimeAdaptionStorageInner {
    pub history: HashMap<GroupId, (usize, usize)>,
    pub iter_cnt: usize,
}

pub struct AdaptiveCostModel {
    runtime_row_cnt: RuntimeAdaptionStorage,
    base_model: DfCostModel,
    decay: usize,
}

impl CostModel<DfNodeType> for AdaptiveCostModel {
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
        node: &DfNodeType,
        predicates: &[ArcDfPredNode],
        children_costs: &[Cost],
        context: Option<RelNodeContext>,
        _optimizer: Option<&CascadesOptimizer<DfNodeType>>,
    ) -> Cost {
        if let DfNodeType::PhysicalScan = node {
            let guard = self.runtime_row_cnt.lock().unwrap();
            if let Some((runtime_row_cnt, iter)) = guard.history.get(&context.unwrap().group_id) {
                if *iter + self.decay >= guard.iter_cnt {
                    let runtime_row_cnt = (*runtime_row_cnt).max(1) as f64;
                    return DfCostModel::cost(runtime_row_cnt, 0.0, runtime_row_cnt);
                } else {
                    return DfCostModel::cost(1.0, 0.0, 1.0);
                }
            } else {
                return DfCostModel::cost(1.0, 0.0, 1.0);
            }
        }
        let (mut row_cnt, compute_cost, io_cost) = DfCostModel::cost_tuple(
            &self
                .base_model
                .compute_cost(node, children_costs, predicates, None, None),
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
        DfCostModel::cost(row_cnt, compute_cost, io_cost)
    }

    fn compute_plan_node_cost(&self, node: &PlanNode<DfNodeType>) -> Cost {
        self.base_model.compute_plan_node_cost(node)
    }
}

impl AdaptiveCostModel {
    pub fn new(decay: usize) -> Self {
        Self {
            runtime_row_cnt: Arc::new(Mutex::new(RuntimeAdaptionStorageInner::default())),
            base_model: DfCostModel::new(HashMap::new()),
            decay,
        }
    }

    pub fn get_runtime_map(&self) -> RuntimeAdaptionStorage {
        self.runtime_row_cnt.clone()
    }
}
