use std::{collections::HashMap, sync::Arc};

use optd_core::{
    cascades::CascadesOptimizer, heuristics::HeuristicsOptimizer, node::Value,
    optimizer::Optimizer, rules::Rule,
};
use optd_datafusion_repr::{
    cost::{DataFusionPerTableStats, OptCostModel},
    plan_nodes::{
        BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, DfPlanNode, JoinType, LogicalFilter,
        LogicalJoin, LogicalScan, OptRelNode, OptRelNodeTyp,
    },
    rules::{HashJoinRule, JoinAssocRule, JoinCommuteRule, PhysicalConversionRule},
};

use tracing::Level;

pub fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_ansi(false)
        .with_target(false)
        .init();

    let transform_rules: Vec<Arc<dyn Rule<OptRelNodeTyp, CascadesOptimizer<OptRelNodeTyp>>>> = vec![
        Arc::new(JoinCommuteRule::new()),
        Arc::new(JoinAssocRule::new()),
    ];
    let impl_rules: Vec<Arc<dyn Rule<OptRelNodeTyp, CascadesOptimizer<OptRelNodeTyp>>>> = vec![
        Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Scan)),
        Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
            JoinType::Inner,
        ))),
        Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Filter)),
        Arc::new(HashJoinRule::new()),
    ];

    let mut optimizer = CascadesOptimizer::new(
        transform_rules.into(),
        impl_rules.into(),
        Arc::new(OptCostModel::new(
            [("t1", 1000), ("t2", 100), ("t3", 10000)]
                .into_iter()
                .map(|(x, y)| {
                    (
                        x.to_string(),
                        DataFusionPerTableStats::new(y, HashMap::new()),
                    )
                })
                .collect(),
        )),
        vec![].into(),
    );

    // The plan: (filter (scan t1) #1=2) join (scan t2) join (scan t3)
    let scan1 = LogicalScan::new("t1".into());
    let filter_cond = BinOpExpr::new(
        ColumnRefExpr::new(1).0,
        ConstantExpr::new(Value::Int64(2)).0,
        BinOpType::Eq,
    );
    let filter1 = LogicalFilter::new(scan1.0, filter_cond.0);
    let scan2 = LogicalScan::new("t2".into());
    let join_cond = ConstantExpr::new(Value::Bool(true));
    let scan3 = LogicalScan::new("t3".into());
    let join_filter = LogicalJoin::new(filter1.0, scan2.0, join_cond.clone().0, JoinType::Inner);
    let fnal = LogicalJoin::new(scan3.0, join_filter.0, join_cond.0, JoinType::Inner);
    let node = optimizer.optimize(fnal.0.clone().into_rel_node());
    // optimizer.dump(None); TODO: implement this function
    let node: Arc<optd_core::node::PlanNode<OptRelNodeTyp>> = node.unwrap();
    println!(
        "cost={}",
        optimizer
            .cost()
            .explain(&optimizer.cost().compute_plan_node_cost(&node))
    );
    println!(
        "{}",
        DfPlanNode::from_rel_node(node)
            .unwrap()
            .explain_to_string(None)
    );

    let mut optimizer = HeuristicsOptimizer::new_with_rules(
        vec![
            Arc::new(JoinCommuteRule::new()),
            Arc::new(JoinAssocRule::new()),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Scan)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
                JoinType::Inner,
            ))),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Filter)),
            Arc::new(HashJoinRule::new()),
        ],
        optd_core::heuristics::ApplyOrder::BottomUp,
        Arc::new([]),
    );
    let node = optimizer.optimize(fnal.0.into_rel_node()).unwrap();
    println!(
        "{}",
        DfPlanNode::from_rel_node(node)
            .unwrap()
            .explain_to_string(None)
    );
}
