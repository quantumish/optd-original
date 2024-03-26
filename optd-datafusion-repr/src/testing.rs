mod dummy_cost;
mod tpch_catalog;

use optd_core::heuristics::{ApplyOrder, HeuristicsOptimizer};

use crate::plan_nodes::OptRelNodeTyp;

/// Create a "dummy" optimizer preloaded with the TPC-H catalog for testing
/// Note: Only provides the schema property currently
pub fn new_dummy_optimizer() -> HeuristicsOptimizer<OptRelNodeTyp> {
    let dummy_optimizer = HeuristicsOptimizer::new_with_rules(vec![], ApplyOrder::TopDown);
    dummy_optimizer
}
