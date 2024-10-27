#![allow(clippy::new_without_default)]

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use cost::{AdaptiveCostModel, DataFusionBaseTableStats, RuntimeAdaptionStorage, DEFAULT_DECAY};
use optd_core::{
    cascades::{CascadesOptimizer, GroupId},
    heuristics::{ApplyOrder, HeuristicsOptimizer},
    nodes::PlanNodeMetaMap,
    optimizer::Optimizer,
    property::PropertyBuilderAny,
    rules::Rule,
};

use plan_nodes::{ArcDfPlanNode, DfNodeType};
use properties::{
    column_ref::ColumnRefPropertyBuilder,
    schema::{Catalog, SchemaPropertyBuilder},
};
use rules::{
    EliminateDuplicatedAggExprRule, EliminateDuplicatedSortExprRule, EliminateFilterRule,
    EliminateJoinRule, EliminateLimitRule, FilterAggTransposeRule, FilterCrossJoinTransposeRule,
    FilterInnerJoinTransposeRule, FilterMergeRule, FilterProjectTransposeRule,
    FilterSortTransposeRule, HashJoinRule, JoinAssocRule, JoinCommuteRule, PhysicalConversionRule,
    ProjectFilterTransposeRule, ProjectMergeRule, ProjectionPullUpJoin, SimplifyFilterRule,
    SimplifyJoinCondRule,
};

pub use optd_core::nodes::Value;

use crate::rules::{
    DepInitialDistinct, DepJoinEliminateAtScan, DepJoinPastAgg, DepJoinPastFilter, DepJoinPastProj,
};

pub mod cost;
mod explain;
pub mod plan_nodes;
pub mod properties;
pub mod rules;
#[cfg(test)]
mod testing;
// mod expand;

pub struct DatafusionOptimizer {
    hueristic_optimizer: HeuristicsOptimizer<DfNodeType>,
    cascades_optimizer: CascadesOptimizer<DfNodeType>,
    pub runtime_statistics: RuntimeAdaptionStorage,
    enable_adaptive: bool,
    enable_heuristic: bool,
}

impl DatafusionOptimizer {
    pub fn enable_adaptive(&mut self, enable: bool) {
        self.enable_adaptive = enable;
    }

    pub fn adaptive_enabled(&self) -> bool {
        self.enable_adaptive
    }

    pub fn enable_heuristic(&mut self, enable: bool) {
        self.enable_heuristic = enable;
    }

    pub fn is_heuristic_enabled(&self) -> bool {
        self.enable_heuristic
    }

    pub fn optd_cascades_optimizer(&self) -> &CascadesOptimizer<DfNodeType> {
        &self.cascades_optimizer
    }

    pub fn optd_hueristic_optimizer(&self) -> &HeuristicsOptimizer<DfNodeType> {
        &self.hueristic_optimizer
    }

    pub fn optd_optimizer_mut(&mut self) -> &mut CascadesOptimizer<DfNodeType> {
        &mut self.cascades_optimizer
    }

    pub fn default_heuristic_rules(
    ) -> Vec<Arc<dyn Rule<DfNodeType, HeuristicsOptimizer<DfNodeType>>>> {
        vec![
            Arc::new(SimplifyFilterRule::new()),
            Arc::new(SimplifyJoinCondRule::new()),
            Arc::new(EliminateFilterRule::new()),
            Arc::new(EliminateJoinRule::new()),
            Arc::new(EliminateLimitRule::new()),
            Arc::new(EliminateDuplicatedSortExprRule::new()),
            Arc::new(EliminateDuplicatedAggExprRule::new()),
            Arc::new(DepJoinEliminateAtScan::new()),
            Arc::new(DepInitialDistinct::new()),
            Arc::new(DepJoinPastProj::new()),
            Arc::new(DepJoinPastFilter::new()),
            Arc::new(DepJoinPastAgg::new()),
            Arc::new(ProjectMergeRule::new()),
            Arc::new(FilterMergeRule::new()),
        ]
    }

    pub fn default_cascades_rules() -> (
        Vec<Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>>,
        Vec<Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>>,
    ) {
        let mut transformation_rules: Vec<
            Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>,
        > = vec![];
        // transformation_rules.push(Arc::new(ProjectFilterTransposeRule::new()));
        // transformation_rules.push(Arc::new(FilterProjectTransposeRule::new()));
        // transformation_rules.push(Arc::new(FilterCrossJoinTransposeRule::new()));
        // transformation_rules.push(Arc::new(FilterInnerJoinTransposeRule::new()));
        // transformation_rules.push(Arc::new(FilterSortTransposeRule::new()));
        // transformation_rules.push(Arc::new(FilterAggTransposeRule::new()));
        // transformation_rules.push(Arc::new(JoinAssocRule::new()));
        transformation_rules.push(Arc::new(JoinCommuteRule::new()));
        transformation_rules.push(Arc::new(ProjectionPullUpJoin::new()));

        let mut implementation_rules: Vec<
            Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>,
        > = PhysicalConversionRule::all_conversions();

        implementation_rules.push(Arc::new(HashJoinRule::new()));

        (transformation_rules, implementation_rules)
    }

    /// Create an optimizer with partial explore (otherwise it's too slow).
    pub fn new_physical(
        catalog: Arc<dyn Catalog>,
        stats: DataFusionBaseTableStats,
        enable_adaptive: bool,
    ) -> Self {
        let (transformation_rules, implementation_rules) = Self::default_cascades_rules();
        let heuristic_rules = Self::default_heuristic_rules();
        let property_builders: Arc<[Box<dyn PropertyBuilderAny<DfNodeType>>]> = Arc::new([
            Box::new(SchemaPropertyBuilder::new(catalog.clone())),
            Box::new(ColumnRefPropertyBuilder::new(catalog.clone())),
        ]);
        let cost_model = AdaptiveCostModel::new(DEFAULT_DECAY, stats);
        Self {
            runtime_statistics: cost_model.get_runtime_map(),
            cascades_optimizer: CascadesOptimizer::new(
                transformation_rules.into(),
                implementation_rules.into(),
                Arc::new(cost_model),
                property_builders.clone(),
            ),
            hueristic_optimizer: HeuristicsOptimizer::new_with_rules(
                heuristic_rules,
                ApplyOrder::TopDown, // uhh TODO reconsider
                property_builders.clone(),
            ),
            enable_adaptive,
            enable_heuristic: true,
        }
    }

    /// The optimizer settings for three-join demo as a perfect optimizer.
    pub fn new_alternative_physical_for_demo(catalog: Arc<dyn Catalog>) -> Self {
        todo!();
        // let rules = PhysicalConversionRule::all_conversions();
        // let mut rule_wrappers = Vec::new();
        // for rule in rules {
        //     rule_wrappers.push(RuleWrapper::new_cascades(rule));
        // }
        // rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(HashJoinRule::new())));
        // rule_wrappers.insert(
        //     0,
        //     RuleWrapper::new_cascades(Arc::new(JoinCommuteRule::new())),
        // );
        // rule_wrappers.insert(1, RuleWrapper::new_cascades(Arc::new(JoinAssocRule::new())));
        // rule_wrappers.insert(
        //     2,
        //     RuleWrapper::new_cascades(Arc::new(ProjectionPullUpJoin::new())),
        // );
        // rule_wrappers.insert(
        //     3,
        //     RuleWrapper::new_heuristic(Arc::new(EliminateFilterRule::new())),
        // );

        // let cost_model =
        //     DataFusionAdaptiveCostModel::new(1000, DataFusionBaseTableStats::default()); // very large decay
        // let runtime_statistics = cost_model.get_runtime_map();
        // let optimizer = CascadesOptimizer::new(
        //     rule_wrappers,
        //     Box::new(cost_model),
        //     vec![
        //         Box::new(SchemaPropertyBuilder::new(catalog.clone())),
        //         Box::new(ColumnRefPropertyBuilder::new(catalog)),
        //     ],
        // );
        // Self {
        //     runtime_statistics,
        //     cascades_optimizer: optimizer,
        //     enable_adaptive: true,
        //     enable_heuristic: false,
        //     hueristic_optimizer: HeuristicsOptimizer::new_with_rules(
        //         vec![],
        //         ApplyOrder::BottomUp,
        //         Arc::new([]),
        //     ),
        // }
    }

    pub fn heuristic_optimize(&mut self, root_rel: ArcDfPlanNode) -> ArcDfPlanNode {
        self.hueristic_optimizer
            .optimize(root_rel)
            .expect("heuristics returns error")
    }

    pub fn cascades_optimize(
        &mut self,
        root_rel: ArcDfPlanNode,
    ) -> Result<(GroupId, ArcDfPlanNode, PlanNodeMetaMap)> {
        if self.enable_adaptive {
            self.runtime_statistics.lock().unwrap().iter_cnt += 1;
            self.cascades_optimizer.step_clear_winner();
        } else {
            self.cascades_optimizer.step_clear();
        }

        let group_id = self.cascades_optimizer.step_optimize_rel(root_rel)?;

        let mut meta = Some(HashMap::new());
        let optimized_rel = self
            .cascades_optimizer
            .step_get_winner(group_id, &mut meta)?;

        Ok((group_id, optimized_rel, meta.unwrap()))
    }

    pub fn dump(&self, group_id: Option<GroupId>) {
        todo!();
        // self.cascades_optimizer.dump(group_id)
    }
}
