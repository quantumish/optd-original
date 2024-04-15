use std::{
    collections::{hash_map::Entry, HashMap, HashSet}, fmt::Display, sync::Arc
};

use anyhow::{bail, Result};
use itertools::Itertools;
use std::any::Any;

use crate::{
    cost::Cost,
    physical_prop::PhysicalPropsBuilder,
    property::PropertyBuilderAny,
    rel_node::{RelNode, RelNodeMeta, RelNodeMetaMap, RelNodeRef, RelNodeTyp, Value},
};

use super::optimizer::{ExprId, GroupId, SubGroupId};

pub type RelMemoNodeRef<T> = Arc<RelMemoNode<T>>;

/// Equivalent to MExpr in Columbia/Cascades.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelMemoNode<T: RelNodeTyp> {
    pub typ: T,
    pub children: Vec<(GroupId,SubGroupId)>,
    pub data: Option<Value>,
}

impl<T: RelNodeTyp> std::fmt::Display for RelMemoNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        if let Some(ref data) = self.data {
            write!(f, " {}", data)?;
        }
        for child in &self.children {
            write!(f, " {}", child)?;
        }
        write!(f, ")")
    }
}

#[derive(Default, Debug, Clone)]
pub struct Winner {
    pub impossible: bool,
    pub expr_id: ExprId,
    pub cost: Cost,
}

#[derive(Default, Debug, Clone)]
pub struct SubGroupInfo {
    pub winner: Option<Winner>,
}

pub(crate) struct SubGroup {
    pub(crate) sub_group_info: SubGroupInfo,
    pub(crate) sub_group_exprs: HashSet<ExprId>,
}

pub(crate) struct Group<T: RelNodeTyp, P: PhysicalPropsBuilder<T>> {
    /// sub_groups are for special requiredPhysicalProps
    ///     all exprs within one sub group are logically equivalent and provide same physical props
    ///     one expr can be in multiple sub groups
    ///     the first sub_group in the group is the default sub group containing all exprs in the group
    ///     eg: a group of join exprs, there are two subgroups, 0 is for all exprs, others are for special requirement
    ///     sub_groups 0: <PhysicalProps:Any, SortMergeJoinExpr, HashJoinExpr, NLJoinExpr>
    ///     sub_groups 1: <PhysicalProps:Sort(a), SortMergeJoinExpr>
    pub(crate) sub_groups: Vec<SubGroup>,
    pub(crate) physical_props: Vec<P::PhysicalProps>,
    pub(crate) sub_group_physical_prop_map: HashMap<P::PhysicalProps, SubGroupId>,
    pub(crate) properties: Arc<[Box<dyn Any + Send + Sync + 'static>]>,
}

impl<T: RelNodeTyp, P: PhysicalPropsBuilder<T>> Group<T, P>{
    pub fn new(physical_props_builder: P) -> Self {
        let mut group = Group::<T, P> {
            sub_groups: Vec::new(),
            physical_props: Vec::new(),
            sub_group_physical_prop_map: HashMap::new(),
            properties: Vec::new(),
        };
        let mut default_sub_group = SubGroup {
            sub_group_info: SubGroupInfo{
                winner: None,
            },
            sub_group_exprs: HashSet::new(),
        };
        group.sub_groups.push(default_sub_group);
        group.physical_props.push(physical_props_builder.Any());
        group.sub_group_physical_prop_map.insert(physical_props_builder.Any(), SubGroupId(0));
        group
    }

    pub fn insert_expr_to_default_sub_group(&mut self, expr_id: ExprId){
        self.sub_groups[0].sub_group_exprs.insert(expr_id);
    }

    pub fn group_exprs(&self) -> &HashSet<ExprId> {
        &(self.sub_groups[0].sub_group_exprs)
    }

    pub fn group_exprs_mut(&mut self) -> &mut HashSet<ExprId> {
        self.sub_groups[0].sub_group_exprs.as_mut()
    }

    pub fn default_sub_group(&self) -> &SubGroup {
        &self.sub_groups[0]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
struct ReducedGroupId(usize);

impl ReducedGroupId {
    pub fn as_group_id(self) -> GroupId {
        GroupId(self.0)
    }
}

impl Display for ReducedGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct Memo<T: RelNodeTyp, P: PhysicalPropsBuilder<T>> {
    expr_id_to_group_id: HashMap<ExprId, GroupId>,
    expr_id_to_expr_node: HashMap<ExprId, RelMemoNodeRef<T>>,
    expr_node_to_expr_id: HashMap<RelMemoNode<T>, ExprId>,
    groups: HashMap<ReducedGroupId, Group<T, P>>,
    group_expr_counter: usize,
    merged_groups: HashMap<GroupId, GroupId>,
    required_root_props: P::PhysicalProps,
    physical_property_builders: Arc<P>,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
}

impl<T: RelNodeTyp, P: PhysicalPropsBuilder<T>> Memo<T, P> {
    pub fn new(property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>, required_root_props: P::PhysicalProps, physical_property_builders: Arc<P>) -> Self {
        Self {
            expr_id_to_group_id: HashMap::new(),
            expr_id_to_expr_node: HashMap::new(),
            expr_node_to_expr_id: HashMap::new(),
            groups: HashMap::new(),
            group_expr_counter: 0,
            merged_groups: HashMap::new(),
            required_root_props,
            physical_property_builders,
            property_builders,
        }
    }

    /// Get the next group id. Group id and expr id shares the same counter, so as to make it easier to debug...
    fn next_group_id(&mut self) -> ReducedGroupId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        ReducedGroupId(id)
    }

    /// Get the next expr id. Group id and expr id shares the same counter, so as to make it easier to debug...
    fn next_expr_id(&mut self) -> ExprId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        ExprId(id)
    }

    fn merge_group_inner(
        &mut self,
        group_a: ReducedGroupId,
        group_b: ReducedGroupId,
    ) -> ReducedGroupId {
        if group_a == group_b {
            return group_a;
        }

        // Copy all expressions from group a to group b
        let group_a_exprs = self.get_all_exprs_in_group(group_a.as_group_id());
        for expr_id in group_a_exprs {
            let expr_node = self.expr_id_to_expr_node.get(&expr_id).unwrap();
            self.add_expr_to_group(expr_id, group_b, expr_node.as_ref().clone());
        }

        self.merged_groups
            .insert(group_a.as_group_id(), group_b.as_group_id());

        // Remove all expressions from group a (so we don't accidentally access it)
        self.clear_exprs_in_group(group_a);

        group_b
    }

    pub fn merge_group(&mut self, group_a: GroupId, group_b: GroupId) -> GroupId {
        let group_a_reduced = self.get_reduced_group_id(group_a);
        let group_b_reduced = self.get_reduced_group_id(group_b);
        self.merge_group_inner(group_a_reduced, group_b_reduced)
            .as_group_id()
    }

    fn get_group_id_of_expr_id(&self, expr_id: ExprId) -> GroupId {
        self.expr_id_to_group_id[&expr_id]
    }

    fn get_reduced_group_id(&self, mut group_id: GroupId) -> ReducedGroupId {
        while let Some(next_group_id) = self.merged_groups.get(&group_id) {
            group_id = *next_group_id;
        }
        ReducedGroupId(group_id.0)
    }

    /// Add or get an expression into the memo, returns the group id and the expr id. If `GroupId` is `None`,
    /// create a new group. Otherwise, add the expression to the group.
    pub fn add_new_group_expr(
        &mut self,
        rel_node: RelNodeRef<T>,
        add_to_group_id: Option<GroupId>,
    ) -> (GroupId, ExprId) {
        let node_current_group = rel_node.typ.extract_group();
        if let (Some(grp_a), Some(grp_b)) = (add_to_group_id, node_current_group) {
            self.merge_group(grp_a, grp_b);
        };

        let (group_id, expr_id) = self.add_new_group_expr_inner(
            rel_node,
            add_to_group_id.map(|x| self.get_reduced_group_id(x)),
        );
        (group_id.as_group_id(), expr_id)
    }

    pub fn get_expr_info(&self, rel_node: RelNodeRef<T>) -> (GroupId, ExprId) {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| {
                if let Some(group) = child.typ.extract_group() {
                    group
                } else {
                    self.get_expr_info(child.clone()).0
                }
            })
            .collect::<Vec<_>>();
        let memo_node = RelMemoNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            data: rel_node.data.clone(),
        };
        let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) else {
            unreachable!("not found {}", memo_node)
        };
        let group_id = self.get_group_id_of_expr_id(expr_id);
        (group_id, expr_id)
    }

    fn infer_properties(
        &self,
        memo_node: RelMemoNode<T>,
    ) -> Vec<Box<dyn Any + 'static + Send + Sync>> {
        let child_properties = memo_node
            .children
            .iter()
            .map(|child| {
                let group_id = self.get_reduced_group_id(*child);
                self.groups[&group_id].properties.clone()
            })
            .collect_vec();
        let mut props = Vec::with_capacity(self.property_builders.len());
        for (id, builder) in self.property_builders.iter().enumerate() {
            let child_properties = child_properties
                .iter()
                .map(|x| x[id].as_ref() as &dyn std::any::Any)
                .collect::<Vec<_>>();
            let prop = builder.derive_any(
                memo_node.typ.clone(),
                memo_node.data.clone(),
                child_properties.as_slice(),
            );
            props.push(prop);
        }
        props
    }

    fn clear_exprs_in_group(&mut self, group_id: ReducedGroupId) {
        self.groups.remove(&group_id);
    }

    /// If group_id exists, it adds expr_id to the existing group
    /// Otherwise, it creates a new group of that group_id and insert expr_id into the new group
    fn add_expr_to_group(
        &mut self,
        expr_id: ExprId,
        group_id: ReducedGroupId,
        memo_node: RelMemoNode<T>,
    ) {
        if let Entry::Occupied(mut entry) = self.groups.entry(group_id) {
            let group = entry.get_mut();
            group.insert_expr_to_default_sub_group(expr_id);
            return;
        }
        let mut group = Group::new(self.required_root_props.Any());
        group.properties = self.infer_properties(memo_node).into();
        group.insert_expr_to_default_sub_group(expr_id);
        self.groups.insert(group_id, group);
    }

    // return true: replace success, the expr_id is replaced by the new rel_node
    // return false: replace failed as the new rel node already exists in other groups,
    //             the old expr_id should be marked as all rules are fired for it
    pub fn replace_group_expr(
        &mut self,
        expr_id: ExprId,
        replace_group_id: GroupId,
        rel_node: RelNodeRef<T>,
    ) -> bool {
        let replace_group_id = self.get_reduced_group_id(replace_group_id);

        if let Entry::Occupied(mut entry) = self.groups.entry(replace_group_id) {
            let group = entry.get_mut();
            if !group.group_exprs().contains(&expr_id) {
                unreachable!("expr not found in group in replace_group_expr");
            }

            let children_group_ids = rel_node
                .children
                .iter()
                .map(|child| {
                    if let Some(group) = child.typ.extract_group() {
                        group
                    } else {
                        self.add_new_group_expr(child.clone(), None).0
                    }
                })
                .collect::<Vec<_>>();

            let memo_node = RelMemoNode {
                typ: rel_node.typ.clone(),
                children: children_group_ids,
                data: rel_node.data.clone(),
            };

            // if the new expr already in the memo table, merge the group and remove old expr
            if let Some(&new_expr_id) = self.expr_node_to_expr_id.get(&memo_node) {
                if new_expr_id == expr_id {
                    // This is not acceptable, as it means the expr returned by a heuristic rule is exactly
                    // the same as the original expr, which should not happen
                    // TODO: we can silently ignore this case without marking the original one as a deadend
                    // But the rule creators should follow the definition of the heuristic rule
                    // and return an empty vec if their rule does not do the real transformation
                    unreachable!("replace_group_expr: you're replacing the old expr with the same expr, please check your rules registered as heuristic
                        and make sure if it does not do any transformation, it should return an empty vec!");
                }
                let group_id = self.get_group_id_of_expr_id(new_expr_id);
                let group_id = self.get_reduced_group_id(group_id);
                self.merge_group_inner(replace_group_id, group_id);
                return false;
            }

            self.expr_id_to_expr_node
                .insert(expr_id, memo_node.clone().into());
            self.expr_node_to_expr_id.insert(memo_node.clone(), expr_id);

            return true;
        }
        unreachable!("group not found in replace_group_expr");
    }

    fn add_new_group_expr_inner(
        &mut self,
        rel_node: RelNodeRef<T>,
        add_to_group_id: Option<ReducedGroupId>,
    ) -> (ReducedGroupId, ExprId) {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| {
                if let Some(group) = child.typ.extract_group() {
                    group
                } else {
                    self.add_new_group_expr(child.clone(), None).0
                }
            })
            .collect::<Vec<_>>();
        let memo_node = RelMemoNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            data: rel_node.data.clone(),
        };
        if let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) {
            let group_id = self.get_group_id_of_expr_id(expr_id);
            let group_id = self.get_reduced_group_id(group_id);
            if let Some(add_to_group_id) = add_to_group_id {
                self.merge_group_inner(add_to_group_id, group_id);
            }
            return (group_id, expr_id);
        }
        let expr_id = self.next_expr_id();
        let group_id = if let Some(group_id) = add_to_group_id {
            group_id
        } else {
            self.next_group_id()
        };
        self.expr_id_to_expr_node
            .insert(expr_id, memo_node.clone().into());
        self.expr_id_to_group_id
            .insert(expr_id, group_id.as_group_id());
        self.expr_node_to_expr_id.insert(memo_node.clone(), expr_id);
        self.add_expr_to_group(expr_id, group_id, memo_node);
        (group_id, expr_id)
    }

    /// Get the group id of an expression.
    /// The group id is volatile, depending on whether the groups are merged.
    pub fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        let group_id = self
            .expr_id_to_group_id
            .get(&expr_id)
            .expect("expr not found in group mapping");
        self.get_reduced_group_id(*group_id).as_group_id()
    }

    /// Get the memoized representation of a node.
    pub fn get_expr_memoed(&self, expr_id: ExprId) -> RelMemoNodeRef<T> {
        self.expr_id_to_expr_node
            .get(&expr_id)
            .expect("expr not found in expr mapping")
            .clone()
    }

    /// Get all bindings of a group.
    /// TODO: this is not efficient. Should decide whether to expand the rule based on the matcher.
    pub fn get_all_group_bindings(
        &self,
        group_id: GroupId,
        physical_only: bool,
        exclude_placeholder: bool,
        level: Option<usize>,
    ) -> Vec<RelNodeRef<T>> {
        let group_id = self.get_reduced_group_id(group_id);
        let group = self.groups.get(&group_id).expect("group not found");
        group
            .group_exprs()
            .iter()
            .filter(|x| !physical_only || !self.get_expr_memoed(**x).typ.is_logical())
            .map(|&expr_id| {
                self.get_all_expr_bindings(expr_id, physical_only, exclude_placeholder, level)
            })
            .concat()
    }

    /// Get all bindings of an expression.
    /// TODO: this is not efficient. Should decide whether to expand the rule based on the matcher.
    pub fn get_all_expr_bindings(
        &self,
        expr_id: ExprId,
        physical_only: bool,
        exclude_placeholder: bool,
        level: Option<usize>,
    ) -> Vec<RelNodeRef<T>> {
        let expr = self.get_expr_memoed(expr_id);
        if let Some(level) = level {
            if level == 0 {
                if exclude_placeholder {
                    return vec![];
                } else {
                    let node = Arc::new(RelNode {
                        typ: expr.typ.clone(),
                        children: expr
                            .children
                            .iter()
                            .map(|x| Arc::new(RelNode::new_group(*x)))
                            .collect_vec(),
                        data: expr.data.clone(),
                    });
                    return vec![node];
                }
            }
        }
        let mut children = vec![];
        let mut cumulative = 1;
        for child in &expr.children {
            let group_exprs = self.get_all_group_bindings(
                *child,
                physical_only,
                exclude_placeholder,
                level.map(|x| x - 1),
            );
            cumulative *= group_exprs.len();
            children.push(group_exprs);
        }
        let mut result = vec![];
        for i in 0..cumulative {
            let mut selected_nodes = vec![];
            let mut ii = i;
            for child in children.iter().rev() {
                let idx = ii % child.len();
                ii /= child.len();
                selected_nodes.push(child[idx].clone());
            }
            selected_nodes.reverse();
            let node = Arc::new(RelNode {
                typ: expr.typ.clone(),
                children: selected_nodes,
                data: expr.data.clone(),
            });
            result.push(node);
        }
        result
    }

    pub fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        let group_id = self.get_reduced_group_id(group_id);
        let group = self.groups.get(&group_id).expect("group not found");
        let mut exprs = group.group_exprs().iter().copied().collect_vec();
        exprs.sort();
        exprs
    }

    pub fn get_all_group_ids(&self) -> Vec<GroupId> {
        let mut ids = self
            .groups
            .keys()
            .copied()
            .map(|x| x.as_group_id())
            .collect_vec();
        ids.sort();
        ids
    }

    pub fn get_sub_group_info(&self, group_id: GroupId,
        required_physical_props: Arc<dyn PhysicalProps<T>>) -> Option<SubGroupInfo<T>> {
        let group = self.groups.get(&self.get_reduced_group_id(group_id)).unwrap();
        let sub_group_id = group.sub_group_physical_prop_map.get(required_physical_props);
        if let Some(sub_group_id) = sub_group_id {
            group.sub_groups[*sub_group_id].sub_group_info.clone()
        }
        None
    }

    pub fn update_expr_children_sub_group_id(&self, expr_id: ExprId, children_props: Vec<Arc<dyn PhysicalProps<T>>>) -> ExprId{
        let memo_node = self.get_expr_memoed(expr_id);
        let children = memo_node.children.iter().zip(children_props.iter()).map(|(child, prop)| {
            let group_id = child.0;
            let group = self.groups.get(&self.get_reduced_group_id(group_id)).unwrap();
            let sub_group_id = group.sub_group_physical_prop_map.get(prop).unwrap();
            (group_id, *sub_group_id)
        }).collect();

        if children == memo_node.children {
            // if there's no required props for children node, sub_group_id remain unchanged (as 0)
            return expr_id;
        }

        let memo_node = RelMemoNode {
            typ: memo_node.typ.clone(),
            children,
            data: memo_node.data.clone(),
        };
        assert!(!self.expr_node_to_expr_id.contains_key(&memo_node));

        let new_expr_id = self.next_expr_id();
        let group_id = self.get_group_id_of_expr_id(expr_id);
        
        self.expr_id_to_expr_node.insert(new_expr_id, memo_node.clone().into());
        self.expr_id_to_group_id.insert(new_expr_id, group_id);
        self.expr_node_to_expr_id.insert(memo_node.clone(), new_expr_id);
        self.get_group(group_id).insert_expr_to_default_sub_group(new_expr_id);
        new_expr_id
    }

    pub fn get_group_info(&self, group_id: GroupId) -> SubGroupInfo<T> {
        self.groups
            .get(&self.get_reduced_group_id(group_id))
            .as_ref()
            .unwrap()
            .default_sub_group()
            .sub_group_info
            .clone()
    }

    pub(crate) fn get_group(&self, group_id: GroupId) -> &Group<T> {
        self.groups
            .get(&self.get_reduced_group_id(group_id))
            .as_ref()
            .unwrap()
    }

    pub fn update_sub_group_info(
        &mut self,
        group_id: GroupId,
        expr_id: Option<ExprId>,
        sub_group_info: SubGroupInfo<T>,
    ) {
        if let Some(ref winner) = sub_group_info.winner {
            if !winner.impossible {
                assert!(
                    winner.cost.0[0] != 0.0,
                    "{}",
                    self.get_expr_memoed(winner.expr_id)
                );
            }
        }
        let group = self.groups.get_mut(&self.get_reduced_group_id(group_id)).unwrap();
        if group.sub_group_physical_prop_map.contains(sub_group_info.physical_props) {
            let sub_group_id = group.sub_group_physical_prop_map.get(&sub_group_info.physical_props).unwrap();
            group.sub_groups[*sub_group_id].sub_group_info = sub_group_info;
            if let Some(expr_id) = expr_id {
                group.sub_groups[*sub_group_id].sub_group_exprs.insert(expr_id);
            }
            return;
        }
        let sub_group_id = group.sub_groups.len();
        let mut exprs = HashSet::new();
        if let Some(expr_id) = expr_id {
            exprs.insert(expr_id);
        }
        group.sub_groups.push(SubGroup {
            sub_group_info,
            sub_group_exprs: exprs,
        });
        group.sub_group_physical_prop_map
            .insert(sub_group_info.physical_props.clone(), SubGroupId(sub_group_id));
    }

    pub fn update_group_info(&mut self, group_id: GroupId, group_info: SubGroupInfo<T>) {
        if let Some(ref winner) = group_info.winner {
            if !winner.impossible {
                assert!(
                    winner.cost.0[0] != 0.0,
                    "{}",
                    self.get_expr_memoed(winner.expr_id)
                );
            }
        }
        let grp = self.groups.get_mut(&self.get_reduced_group_id(group_id));
        grp.unwrap().sub_groups[0].sub_group_info = group_info;
    }

    pub fn get_best_group_binding(
        &self,
        group_id: GroupId,
        meta: &mut Option<RelNodeMetaMap>,
    ) -> Result<RelNodeRef<T>> {
        let info = self.get_group_info(group_id);
        if let Some(winner) = info.winner {
            if !winner.impossible {
                let expr_id = winner.expr_id;
                let expr = self.get_expr_memoed(expr_id);
                let mut children = Vec::with_capacity(expr.children.len());
                for child in &expr.children {
                    children.push(self.get_best_group_binding(*child, meta)?);
                }
                let node = Arc::new(RelNode {
                    typ: expr.typ.clone(),
                    children,
                    data: expr.data.clone(),
                });

                if let Some(meta) = meta {
                    meta.insert(
                        node.as_ref() as *const _ as usize,
                        RelNodeMeta::new(group_id, winner.cost),
                    );
                }
                return Ok(node);
            }
        }
        bail!("no best group binding for group {}", group_id)
    }

    pub fn clear_winner(&mut self) {
        for group in self.groups.values_mut() {
            group.info.winner = None;
        }
    }

    /// Return number of expressions in the memo table.
    pub fn compute_plan_space(&self) -> usize {
        self.expr_id_to_expr_node.len()
    }
}
