mod dummy_cost;
mod tpch_catalog;

use std::sync::Arc;

pub use dummy_cost::DummyCostModel;
use optd_core::{cascades::CascadesOptimizer, optimizer::Optimizer};
pub use tpch_catalog::TpchCatalog;

use crate::{plan_nodes::OptRelNodeTyp, properties::schema::SchemaPropertyBuilder};

/// Create a "dummy" optimizer preloaded with the TPC-H catalog for testing
/// Note: Only provides the schema property currently
pub fn new_dummy_optimizer() -> impl Optimizer<OptRelNodeTyp> {
    let dummy_catalog = Arc::new(TpchCatalog);
    let dummy_optimizer = CascadesOptimizer::new(
        vec![],
        Box::new(DummyCostModel),
        vec![Box::new(SchemaPropertyBuilder::new(dummy_catalog))],
    );

    dummy_optimizer
}
