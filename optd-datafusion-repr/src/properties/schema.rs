use datafusion_expr::EmptyRelation;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use optd_core::{nodes::ArcPredNode, property::PropertyBuilder};

use super::DEFAULT_NAME;
use crate::plan_nodes::{
    decode_empty_relation_schema, ArcDfPredNode, ConstantPred, ConstantType, DfNodeType,
    DfReprPredNode, FuncType,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub typ: ConstantType,
    pub nullable: bool,
}

impl Field {
    /// Generate a field that is only a place holder whose members are never used.
    fn placeholder() -> Self {
        Self {
            name: DEFAULT_NAME.to_string(),
            typ: ConstantType::Binary,
            nullable: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait Catalog: Send + Sync + 'static {
    fn get(&self, name: &str) -> Schema;
}

pub struct SchemaPropertyBuilder {
    catalog: Arc<dyn Catalog>,
}

impl SchemaPropertyBuilder {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    fn derive_for_predicate(predicate: ArcDfPredNode) -> Schema {
        let children = predicate.children();
        let data = predicate.data();
        match predicate {
            DfNodeType::ColumnRef => {
                let data_typ = ConstantType::get_data_type_from_value(&data.unwrap());
                Schema {
                    fields: vec![Field {
                        name: DEFAULT_NAME.to_string(),
                        typ: data_typ,
                        nullable: true,
                    }],
                }
            }
            DfNodeType::List => {
                let mut fields = vec![];
                for child in children {
                    fields.extend(child.fields.clone());
                }
                Schema { fields }
            }
            DfNodeType::LogOp(_) => Schema {
                fields: vec![Field::placeholder(); children.len()],
            },
            DfNodeType::Agg => {
                let mut group_by_schema = children[1].clone();
                let agg_schema = children[2].clone();
                group_by_schema.fields.extend(agg_schema.fields);
                group_by_schema
            }
            DfNodeType::Cast => Schema {
                fields: children[0]
                    .fields
                    .iter()
                    .map(|field| Field {
                        typ: children[1].fields[0].typ,
                        ..field.clone()
                    })
                    .collect(),
            },
            DfNodeType::DataType(data_type) => Schema {
                fields: vec![Field {
                    // name and nullable are just placeholders since
                    // they'll be overwritten by Cast
                    name: DEFAULT_NAME.to_string(),
                    typ: ConstantType::from_data_type(data_type),
                    nullable: true,
                }],
            },
            DfNodeType::Func(FuncType::Agg(_)) => Schema {
                // TODO: this is just a place holder now.
                // The real type should be the column type.
                fields: vec![Field::placeholder()],
            },
            _ => Schema { fields: vec![] },
        }
    }
}

impl PropertyBuilder<DfNodeType> for SchemaPropertyBuilder {
    type Prop = Schema;

    fn derive(
        &self,
        typ: DfNodeType,
        predicates: &[ArcDfPredNode],
        children: &[&Self::Prop],
    ) -> Self::Prop {
        match typ {
            DfNodeType::Scan => {
                let table_name = ConstantPred::from_pred_node(predicates[0])
                    .unwrap()
                    .as_str();
                self.catalog.get(&table_name)
            }
            DfNodeType::Projection => self.derive_schema_for_predicate(predicates[0].clone()),
            DfNodeType::Filter => children[0].clone(),
            DfNodeType::RawDepJoin(join_type)
            | DfNodeType::Join(join_type)
            | DfNodeType::DepJoin(join_type) => {
                use crate::plan_nodes::JoinType::*;
                match join_type {
                    Inner | LeftOuter | RightOuter | FullOuter | Cross => {
                        let mut schema = children[0].clone();
                        let schema2 = children[1].clone();
                        schema.fields.extend(schema2.fields);
                        schema
                    }
                    LeftSemi | LeftAnti => children[0].clone(),
                    RightSemi | RightAnti => children[1].clone(),
                }
            }
            DfNodeType::EmptyRelation => decode_empty_relation_schema(&predicates[1]),
            x => unimplemented!("cannot derive schema property for {}", x),
        }
    }

    fn property_name(&self) -> &'static str {
        "schema"
    }
}
