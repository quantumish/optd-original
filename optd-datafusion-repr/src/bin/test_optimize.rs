use std::sync::Arc;

use optd_core::{
    cascades::CascadesOptimizer, heuristics::HeuristicsOptimizer, nodes::Value,
    optimizer::Optimizer, rules::Rule,
};
use optd_datafusion_repr::{
    cost::DfCostModel,
    plan_nodes::{
        BinOpPred, BinOpType, ColumnRefPred, ConstantPred, DfNodeType, DfReprPlanNode,
        DfReprPlanNode, JoinType, LogicalFilter, LogicalJoin, LogicalScan,
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

    let transform_rules: Vec<Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>> = vec![
        Arc::new(JoinCommuteRule::new()),
        Arc::new(JoinAssocRule::new()),
    ];
    let impl_rules: Vec<Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>> = vec![
        Arc::new(PhysicalConversionRule::new(DfNodeType::Scan)),
        Arc::new(PhysicalConversionRule::new(DfNodeType::Join(
            JoinType::Inner,
        ))),
        Arc::new(PhysicalConversionRule::new(DfNodeType::Filter)),
        Arc::new(HashJoinRule::new()),
    ];

    let mut optimizer = CascadesOptimizer::new(
        transform_rules.into(),
        impl_rules.into(),
        Arc::new(DfCostModel::new(
            [("t1", 1000), ("t2", 100), ("t3", 10000)]
                .into_iter()
                .map(|(x, y)| (x.to_string(), y))
                .collect(),
        )),
        vec![].into(),
    );

    // The plan: (filter (scan t1) #1=2) join (scan t2) join (scan t3)
    let scan1 = LogicalScan::new("t1".into());
    let filter_cond = BinOpPred::new(
        ColumnRefPred::new(1).0,
        ConstantPred::new(Value::Int64(2)).0,
        BinOpType::Eq,
    );
    let filter1 = LogicalFilter::new(scan1.0, filter_cond.0);
    let scan2 = LogicalScan::new("t2".into());
    let join_cond = ConstantPred::new(Value::Bool(true));
    let scan3 = LogicalScan::new("t3".into());
    let join_filter = LogicalJoin::new(filter1.0, scan2.0, join_cond.clone().0, JoinType::Inner);
    let fnal = LogicalJoin::new(scan3.0, join_filter.0, join_cond.0, JoinType::Inner);
    let node = optimizer.optimize(fnal.0.clone().into_rel_node());
    // optimizer.dump(None); TODO: implement this function
    let node: Arc<optd_core::nodes::PlanNode<DfNodeType>> = node.unwrap();
    println!(
        "cost={}",
        optimizer
            .cost()
            .explain(&optimizer.cost().compute_plan_node_cost(&node))
    );
    println!(
        "{}",
        DfReprPlanNode::from_rel_node(node)
            .unwrap()
            .explain_to_string(None)
    );

    let mut optimizer = HeuristicsOptimizer::new_with_rules(
        vec![
            Arc::new(JoinCommuteRule::new()),
            Arc::new(JoinAssocRule::new()),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Scan)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Join(
                JoinType::Inner,
            ))),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Filter)),
            Arc::new(HashJoinRule::new()),
        ],
        optd_core::heuristics::ApplyOrder::BottomUp,
        Arc::new([]),
    );
    let node = optimizer.optimize(fnal.0.into_rel_node()).unwrap();
    println!(
        "{}",
        DfReprPlanNode::from_rel_node(node)
            .unwrap()
            .explain_to_string(None)
    );
}
