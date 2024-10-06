use std::collections::HashMap;
use std::sync::Arc;

use optd_core::optimizer::Optimizer;
use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::{BinOpType, JoinType, OptRelNodeTyp};

pub struct PhysicalConversionRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

impl PhysicalConversionRule {
    pub fn new(logical_typ: OptRelNodeTyp) -> Self {
        Self {
            matcher: RuleMatcher::MatchAndPickNode {
                typ: logical_typ,
                pick_to: 0,
                children: vec![RuleMatcher::IgnoreMany],
            },
        }
    }
}

impl PhysicalConversionRule {
    pub fn all_conversions<O: Optimizer<OptRelNodeTyp>>() -> Vec<Arc<dyn Rule<OptRelNodeTyp, O>>> {
        // Define conversions below, and add them to this list!
        let mut rules: Vec<Arc<dyn Rule<OptRelNodeTyp, O>>> = vec![
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Scan)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Projection)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
                JoinType::Inner,
            ))),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
                JoinType::LeftOuter,
            ))),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
                JoinType::Cross,
            ))),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Filter)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Sort)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Agg)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::EmptyRelation)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Limit)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::ColumnRef)),
        ];

        static BIN_OP_TYPES: [BinOpType; 11] = [
            BinOpType::Add,
            BinOpType::Sub,
            BinOpType::Mul,
            BinOpType::Div,
            BinOpType::Mod,
            BinOpType::Eq,
            BinOpType::Neq,
            BinOpType::Gt,
            BinOpType::Lt,
            BinOpType::Geq,
            BinOpType::Leq,
        ];
        for bin_op_type in BIN_OP_TYPES.into_iter() {
            rules.push(Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::BinOp(
                bin_op_type,
            ))));
        }

        rules
    }
}

impl<O: Optimizer<OptRelNodeTyp>> Rule<OptRelNodeTyp, O> for PhysicalConversionRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        _optimizer: &O,
        mut input: HashMap<usize, RelNode<OptRelNodeTyp>>,
    ) -> Vec<RelNode<OptRelNodeTyp>> {
        let RelNode {
            typ,
            data,
            children,
        } = input.remove(&0).unwrap();

        match typ {
            OptRelNodeTyp::Apply(x) => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalNestedLoopJoin(x.to_join_type()),
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Join(x) => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalNestedLoopJoin(x),
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Scan => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalScan,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Filter => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalFilter,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Projection => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalProjection,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Sort => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalSort,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Agg => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalAgg,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::EmptyRelation => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalEmptyRelation,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Limit => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalLimit,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::ColumnRef => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalColumnRef,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::BinOp(x) => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalBinOp(x),
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
