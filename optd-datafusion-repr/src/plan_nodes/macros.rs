macro_rules! define_plan_node {
    (
        $struct_name:ident : $meta_typ:tt,
        $variant:ident,
        [ $({ $child_id:literal, $child_name:ident : $child_meta_typ:ty }),* ] ,
        [ $({ $attr_id:literal, $attr_name:ident : $attr_meta_typ:ty }),* ]
        $(, { $inner_name:ident : $inner_typ:ty })?
    ) => {
        impl DfReprPlanNode for $struct_name {
            fn into_plan_node(self) -> ArcDfPlanNode {
                self.0
            }

            fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self> {
                #[allow(unused_variables)]
                if let DfNodeType :: $variant $( ($inner_name) )? = plan_node.typ {
                    Some(Self(plan_node))
                } else {
                    None
                }
            }

            fn explain(&self, meta_map: Option<&crate::PlanNodeMetaMap>) -> pretty_xmlish::Pretty<'static> {
                use crate::explain::Insertable;

                let mut fields = vec![
                    $( (stringify!($inner_name), self.$inner_name().to_string().into() ) , )?
                    $( (stringify!($attr_name), dispatch_explain(self.$attr_name(), meta_map) ) ),*
                ];
                if let Some(meta_map) = meta_map {
                    fields = fields.with_meta(get_meta(self.0, meta_map));
                };

                pretty_xmlish::Pretty::simple_record(
                    stringify!($struct_name),
                    fields,
                    vec![
                        $( dispatch_explain(self.$child_name(), meta_map) ),*
                    ],
                )
            }
        }

        impl $struct_name {
            pub fn new(
                $($child_name : $child_meta_typ,)*
                $($attr_name : $attr_meta_typ),*
                $(, $inner_name : $inner_typ)?
            ) -> $struct_name {
                #[allow(unused_mut, unused)]
                $struct_name(
                    DfPlanNode {
                        typ: DfNodeType::$variant $( ($inner_name) )?,
                        children: vec![
                            $(PlanNodeOrGroup::PlanNode($child_name),)*
                        ],
                        predicates: vec![
                            $($attr_name.into_pred_node()),*
                        ],
                    }
                    .into(),
                )
            }

            $(
                pub fn $child_name(&self) -> $child_meta_typ {
                    self.0.child($child_id)
                }
            )*


            $(
                pub fn $attr_name(&self) -> $attr_meta_typ {
                    self.0.predicate($attr_id)
                }
            )*

            $(
                pub fn $inner_name(&self) -> JoinType {
                    if let DfNodeType :: $variant ($inner_name) = self.0 .0.typ {
                        return $inner_name;
                    } else {
                        unreachable!();
                    }
                }
            )?
        }
    };
    // Dummy branch that does nothing when data is `None`.
    (@expand_data_fields $self:ident, $struct_name:ident, $fields:ident) => {};
    // Expand explain fields with data.
    (@expand_data_fields $self:ident, $struct_name:ident, $fields:ident, $data_typ:ty) => {
        let value = $self.0 .0.data.as_ref().unwrap();
        $fields.extend($struct_name::explain_data(&value.into()));
    };
}

pub(crate) use define_plan_node;

#[cfg(test)]
mod test {
    use crate::plan_nodes::*;

    fn get_explain_str(pretty: &Pretty) -> String {
        let mut config = PrettyConfig {
            need_boundaries: false,
            reduced_spaces: false,
            width: 300,
            ..Default::default()
        };
        let mut out = String::new();
        config.unicode(&mut out, pretty);
        out
    }
}
