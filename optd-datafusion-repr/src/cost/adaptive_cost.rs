use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use optd_core::{
    cascades::{CascadesOptimizer, GroupId, NaiveMemo, RelNodeContext},
    cost::{Cost, CostModel, Statistics},
    nodes::Value,
};

use crate::plan_nodes::DfNodeType;

use super::{base_cost::DEFAULT_TABLE_ROW_CNT, DfCostModel};

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

impl AdaptiveCostModel {
    fn get_row_cnt(&self, _: &Option<Value>, context: &Option<RelNodeContext>) -> f64 {
        let guard = self.runtime_row_cnt.lock().unwrap();
        if let Some((runtime_row_cnt, iter)) =
            guard.history.get(&context.as_ref().unwrap().group_id)
        {
            if *iter + self.decay >= guard.iter_cnt {
                return (*runtime_row_cnt).max(1) as f64;
            }
        }
        DEFAULT_TABLE_ROW_CNT as f64
    }
}

impl CostModel<DfNodeType, NaiveMemo<DfNodeType>> for AdaptiveCostModel {
    fn explain_cost(&self, cost: &Cost) -> String {
        self.base_model.explain_cost(cost)
    }

    fn explain_statistics(&self, cost: &Statistics) -> String {
        self.base_model.explain_statistics(cost)
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {
        self.base_model.accumulate(total_cost, cost)
    }

    fn zero(&self) -> Cost {
        self.base_model.zero()
    }

    fn weighted_cost(&self, cost: &Cost) -> f64 {
        self.base_model.weighted_cost(cost)
    }

    fn compute_operation_cost(
        &self,
        node: &DfNodeType,
        data: &Option<Value>,
        children: &[Option<&Statistics>],
        children_cost: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType, NaiveMemo<DfNodeType>>>,
    ) -> Cost {
        if let DfNodeType::PhysicalScan = node {
            let row_cnt = self.get_row_cnt(data, &context);
            return DfCostModel::cost(0.0, row_cnt);
        }
        self.base_model.compute_operation_cost(
            node,
            data,
            children,
            children_cost,
            context,
            optimizer,
        )
    }

    fn derive_statistics(
        &self,
        node: &DfNodeType,
        data: &Option<Value>,
        children: &[&Statistics],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType, NaiveMemo<DfNodeType>>>,
    ) -> Statistics {
        if let DfNodeType::PhysicalScan = node {
            let row_cnt = self.get_row_cnt(data, &context);
            return DfCostModel::stat(row_cnt);
        }
        self.base_model
            .derive_statistics(node, data, children, context, optimizer)
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
