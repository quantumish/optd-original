use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType;
use optd_core::nodes::PlanNode;
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::{
    BinOpType, ConstantType, DfNodeType, FuncType, JoinType, LogOpType, SortOrderType, UnOpType,
};

pub struct PhysicalConversionRule {
    matcher: RuleMatcher<DfNodeType>,
}

impl PhysicalConversionRule {
    pub fn new(logical_typ: DfNodeType) -> Self {
        Self {
            matcher: RuleMatcher::MatchAndPickDiscriminant {
                typ_discriminant: std::mem::discriminant(&logical_typ),
                children: vec![RuleMatcher::IgnoreMany],
                pick_to: 0,
            },
        }
    }
}

impl PhysicalConversionRule {
    pub fn all_conversions<O: Optimizer<DfNodeType>>() -> Vec<Arc<dyn Rule<DfNodeType, O>>> {
        // Define conversions below, and add them to this list!
        // Note that we're using discriminant matching, so only one value of each variant
        // is sufficient to match all values of a variant.
        let mut rules: Vec<Arc<dyn Rule<DfNodeType, O>>> = vec![
            Arc::new(PhysicalConversionRule::new(DfNodeType::Scan)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Projection)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Join(
                JoinType::Inner,
            ))),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Filter)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Sort)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Agg)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::EmptyRelation)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Limit)),
        ];

        rules
    }
}

impl<O: Optimizer<DfNodeType>> Rule<DfNodeType, O> for PhysicalConversionRule {
    fn matcher(&self) -> &RuleMatcher<DfNodeType> {
        &self.matcher
    }

    fn apply(
        &self,
        _optimizer: &O,
        mut input: HashMap<usize, PlanNode<DfNodeType>>,
    ) -> Vec<PlanNode<DfNodeType>> {
        let PlanNode {
            typ,
            data,
            children,
        } = input.remove(&0).unwrap();

        match typ {
            DfNodeType::Apply(x) => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalNestedLoopJoin(x.to_join_type()),
                    children,
                    data,
                };
                vec![node]
            }
            DfNodeType::Join(x) => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalNestedLoopJoin(x),
                    children,
                    data,
                };
                vec![node]
            }
            DfNodeType::Scan => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalScan,
                    children,
                    data,
                };
                vec![node]
            }
            DfNodeType::Filter => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalFilter,
                    children,
                    data,
                };
                vec![node]
            }
            DfNodeType::Projection => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalProjection,
                    children,
                    data,
                };
                vec![node]
            }
            DfNodeType::Sort => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalSort,
                    children,
                    data,
                };
                vec![node]
            }
            DfNodeType::Agg => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalAgg,
                    children,
                    data,
                };
                vec![node]
            }
            DfNodeType::EmptyRelation => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalEmptyRelation,
                    children,
                    data,
                };
                vec![node]
            }
            DfNodeType::Limit => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalLimit,
                    children,
                    data,
                };
                vec![node]
            }
            _ => vec![],
        }
    }

    fn is_impl_rule(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "physical_conversion"
    }
}
