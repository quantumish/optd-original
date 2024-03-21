use crate::explain::Insertable;

use super::{
    macros::define_plan_node, Expr, ExprList, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode,
};

#[derive(Clone, Debug)]
pub struct LogicalScan(pub PlanNode);

define_plan_node!(
    LogicalScan : PlanNode,
    Scan, [], [
        { 0, table_name: Expr },
        { 1, filter: Expr },
        { 2, projections: ExprList },
        { 3, fetch: Expr }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalScan(pub PlanNode);

define_plan_node!(
    PhysicalScan : PlanNode,
    Scan, [], [
        { 0, table_name: Expr },
        { 1, filter: Expr },
        { 2, projections: ExprList },
        { 3, fetch: Expr }
    ]
);

// impl OptRelNode for LogicalScan {
//     fn into_rel_node(self) -> OptRelNodeRef {
//         self.0.into_rel_node()
//     }

//     fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
//         if rel_node.typ != OptRelNodeTyp::Scan {
//             return None;
//         }
//         PlanNode::from_rel_node(rel_node).map(Self)
//     }

//     fn dispatch_explain(&self, _meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
//         Pretty::childless_record(
//             "LogicalScan",
//             vec![("table", self.table().to_string().into())],
//         )
//     }
// }

// impl LogicalScan {
//     pub fn new(table: String, filter: Expr, projections: ExprList) -> LogicalScan {
//         LogicalScan(PlanNode(
//             RelNode {
//                 typ: OptRelNodeTyp::Scan,
//                 children: vec![filter, projections],
//                 data: Some(Value::String(table.into())),
//             }
//             .into(),
//         ))
//     }

//     pub fn table(&self) -> Arc<str> {
//         self.clone().into_rel_node().data.as_ref().unwrap().as_str()
//     }
// }

// #[derive(Clone, Debug)]
// pub struct PhysicalScan(pub PlanNode);

// impl OptRelNode for PhysicalScan {
//     fn into_rel_node(self) -> OptRelNodeRef {
//         replace_typ(self.0.into_rel_node(), OptRelNodeTyp::PhysicalScan)
//     }

//     fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
//         if rel_node.typ != OptRelNodeTyp::PhysicalScan {
//             return None;
//         }
//         PlanNode::from_rel_node(rel_node).map(Self)
//     }

//     fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
//         let mut fields = vec![("table", self.table().to_string().into())];
//         if let Some(meta_map) = meta_map {
//             fields = fields.with_meta(self.0.get_meta(meta_map));
//         }
//         Pretty::simple_record("PhysicalScan", fields, self.0.into_rel_node().children())
//     }
// }

// impl PhysicalScan {
//     pub fn new(node: PlanNode) -> PhysicalScan {
//         Self(node)
//     }

//     pub fn table(&self) -> Arc<str> {
//         self.clone().into_rel_node().data.as_ref().unwrap().as_str()
//     }
// }
