use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use itertools::Itertools;
use std::any::Any;

use crate::{
    nodes::{ArcPlanNode, NodeType, PlanNode, PlanNodeOrGroup},
    optimizer::Optimizer,
    property::PropertyBuilderAny,
    rules::{Rule, RuleMatcher},
};

pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

pub struct HeuristicsOptimizer<T: NodeType> {
    rules: Arc<[Arc<dyn Rule<T, Self>>]>,
    apply_order: ApplyOrder,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    properties: HashMap<ArcPlanNode<T>, Arc<[Box<dyn Any + Send + Sync + 'static>]>>,
}

fn match_node<T: NodeType>(
    typ: &T,
    children: &[RuleMatcher<T>],
    pick_to: Option<usize>,
    node: ArcPlanNode<T>,
) -> Option<HashMap<usize, PlanNode<T>>> {
    if let RuleMatcher::PickMany { .. } | RuleMatcher::IgnoreMany = children.last().unwrap() {
    } else {
        assert_eq!(
            children.len(),
            node.children.len(),
            "children size unmatched, please fix the rule: {}",
            node
        );
    }

    let mut should_end = false;
    let mut pick: HashMap<usize, PlanNode<T>> = HashMap::new();
    for (idx, child) in children.iter().enumerate() {
        assert!(!should_end, "many matcher should be at the end");
        match child {
            RuleMatcher::IgnoreOne => {}
            RuleMatcher::IgnoreMany => {
                should_end = true;
            }
            RuleMatcher::PickOne { pick_to, expand: _ } => {
                // Heuristics always keep the full plan without group placeholders, therefore we can ignore expand property.
                let res = pick.insert(
                    *pick_to,
                    Arc::unwrap_or_clone(node.child(idx).unwrap_plan_node()),
                );
                assert!(res.is_none(), "dup pick");
            }
            RuleMatcher::PickMany { pick_to } => {
                panic!("PickMany not supported currently");
                // let res = pick.insert(*pick_to, PlanNode::new_list(node.children[idx..].to_vec()));
                // assert!(res.is_none(), "dup pick");
                // should_end = true;
            }
            _ => {
                if let Some(new_picks) = match_and_pick(child, node.child(idx).unwrap_plan_node()) {
                    pick.extend(new_picks.iter().map(|(k, v)| (*k, v.clone().into())));
                } else {
                    return None;
                }
            }
        }
    }
    if let Some(pick_to) = pick_to {
        let res: Option<PlanNode<T>> = pick.insert(
            pick_to,
            PlanNode {
                typ: typ.clone(),
                children: node.children.clone(),
                predicates: node.predicates.clone(),
            },
        );
        assert!(res.is_none(), "dup pick");
    }
    Some(pick)
}

fn match_and_pick<T: NodeType>(
    matcher: &RuleMatcher<T>,
    node: ArcPlanNode<T>,
) -> Option<HashMap<usize, PlanNode<T>>> {
    match matcher {
        RuleMatcher::MatchAndPickNode {
            typ,
            children,
            pick_to,
        } => {
            if &node.typ != typ {
                return None;
            }
            match_node(typ, children, Some(*pick_to), node)
        }
        RuleMatcher::MatchNode { typ, children } => {
            if &node.typ != typ {
                return None;
            }
            match_node(typ, children, None, node)
        }
        _ => panic!("top node should be match node"),
    }
}

impl<T: NodeType> HeuristicsOptimizer<T> {
    pub fn new_with_rules(
        rules: Vec<Arc<dyn Rule<T, Self>>>,
        apply_order: ApplyOrder,
        property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    ) -> Self {
        Self {
            rules: rules.into(),
            apply_order,
            property_builders,
            properties: HashMap::new(),
        }
    }

    fn optimize_inputs(&mut self, inputs: &[PlanNodeOrGroup<T>]) -> Result<Vec<ArcPlanNode<T>>> {
        let mut optimized_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            let input = input.unwrap_plan_node();
            optimized_inputs.push(self.optimize(input.clone())?);
        }
        Ok(optimized_inputs)
    }

    fn apply_rules(&mut self, mut root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        for rule in self.rules.as_ref() {
            let matcher = rule.matcher();
            if let Some(picks) = match_and_pick(matcher, root_rel.clone()) {
                let picks = picks
                    .into_iter()
                    .map(|(k, v)| (k, PlanNodeOrGroup::PlanNode(v.into())))
                    .collect(); // This is kinda ugly, but it works for now
                let mut results = rule.apply(self, picks);
                assert!(results.len() <= 1);
                if !results.is_empty() {
                    root_rel = results.remove(0).into();
                }
            }
        }
        Ok(root_rel)
    }

    fn optimize_inner(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        match self.apply_order {
            ApplyOrder::BottomUp => {
                let optimized_children = self
                    .optimize_inputs(&root_rel.children)?
                    .into_iter()
                    .map(|x| PlanNodeOrGroup::PlanNode(x))
                    .collect();
                let node = self.apply_rules(
                    PlanNode {
                        typ: root_rel.typ.clone(),
                        children: optimized_children,
                        predicates: root_rel.predicates.clone(),
                    }
                    .into(),
                )?;
                self.infer_properties(root_rel.clone());
                self.properties.insert(
                    node.clone(),
                    self.properties.get(&root_rel.clone()).unwrap().clone(),
                );
                Ok(node)
            }
            ApplyOrder::TopDown => {
                self.infer_properties(root_rel.clone());
                let root_rel = self.apply_rules(root_rel)?;
                let optimized_children = self
                    .optimize_inputs(&root_rel.children)?
                    .into_iter()
                    .map(|x| PlanNodeOrGroup::PlanNode(x))
                    .collect();
                let node: Arc<PlanNode<T>> = PlanNode {
                    typ: root_rel.typ.clone(),
                    children: optimized_children,
                    predicates: root_rel.predicates.clone(),
                }
                .into();
                self.infer_properties(root_rel.clone());
                self.properties.insert(
                    node.clone(),
                    self.properties.get(&root_rel.clone()).unwrap().clone(),
                );
                Ok(node)
            }
        }
    }

    fn infer_properties(&mut self, root_rel: ArcPlanNode<T>) {
        if self.properties.contains_key(&root_rel) {
            return;
        }

        let child_properties = root_rel
            .children
            .iter()
            .map(|child| {
                let plan_node = child.unwrap_plan_node();
                self.infer_properties(plan_node.clone());
                self.properties.get(&plan_node).unwrap().clone()
            })
            .collect_vec();
        let mut props = Vec::with_capacity(self.property_builders.len());
        for (id, builder) in self.property_builders.iter().enumerate() {
            let child_properties = child_properties
                .iter()
                .map(|x| x[id].as_ref() as &dyn std::any::Any)
                .collect::<Vec<_>>();
            let prop = builder.derive_any(root_rel.typ.clone(), child_properties.as_slice());
            props.push(prop);
        }
        self.properties.insert(root_rel.clone(), props.into());
    }
}

impl<T: NodeType> Optimizer<T> for HeuristicsOptimizer<T> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        self.optimize_inner(root_rel)
    }

    fn get_property<P: crate::property::PropertyBuilder<T>>(
        &self,
        root_rel: ArcPlanNode<T>,
        idx: usize,
    ) -> P::Prop {
        let props = self.properties.get(&root_rel).unwrap();
        let prop = props[idx].as_ref();
        prop.downcast_ref::<P::Prop>().unwrap().clone()
    }
}
