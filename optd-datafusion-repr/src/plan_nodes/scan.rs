use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::explain::Insertable;
use optd_core::nodes::{PlanNode, PlanNodeMetaMap, PredNode, Value};

use super::{
    get_meta, ArcDfPlanNode, ConstantPred, DfNodeType, DfPlanNode, DfPredType, DfReprPlanNode,
    DfReprPredNode,
};

#[derive(Clone, Debug)]
pub struct LogicalScan(pub ArcDfPlanNode);

impl DfReprPlanNode for LogicalScan {
    fn into_plan_node(self) -> ArcDfPlanNode {
        self.0
    }

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self> {
        if plan_node.typ != DfNodeType::Scan {
            return None;
        }
        Some(Self(plan_node))
    }

    fn explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::childless_record(
            "LogicalScan",
            vec![("table", self.table().to_string().into())],
        )
    }
}

impl LogicalScan {
    pub fn new(table: String) -> LogicalScan {
        LogicalScan(
            DfPlanNode {
                typ: DfNodeType::Scan,
                children: vec![],
                predicates: vec![ConstantPred::string(table).into_pred_node()],
            }
            .into(),
        )
    }

    pub fn table(&self) -> Arc<str> {
        self.0
            .predicates
            .first()
            .unwrap()
            .data
            .as_ref()
            .unwrap()
            .as_str()
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalScan(pub ArcDfPlanNode);

impl DfReprPlanNode for PhysicalScan {
    fn into_plan_node(self) -> ArcDfPlanNode {
        self.0
    }

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self> {
        if plan_node.typ != DfNodeType::PhysicalScan {
            return None;
        }
        Some(Self(plan_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        let mut fields = vec![("table", self.table().to_string().into())];
        if let Some(meta_map) = meta_map {
            fields = fields.with_meta(get_meta(&self.0, meta_map));
        }
        Pretty::childless_record("PhysicalScan", fields)
    }
}

impl PhysicalScan {
    pub fn table(&self) -> Arc<str> {
        ConstantPred::from_pred_node(self.0.predicates.first().unwrap().clone())
            .unwrap()
            .value()
            .as_str()
    }
}
