# Properties

In optd_og, properties are defined by implementing the `PropertyBuilder` trait in `optd_og-core/src/property.rs`. Properties will be automatically inferred when plan nodes are added to the memo table. When initializing an optimizer instance, developers will need to provide a vector of properties the optimizer will need to compute throughout the optimization process.

## Define a Property

Currently, optd_og only supports logical properties. It cannot optimize a query plan with required physical properties for now. An example of property definition is the Datafusion representation's plan node schema, as in `optd_og-datafusion-repr/src/properties/schema.rs`.


```rust
impl PropertyBuilder<OptRelNodeTyp> for SchemaPropertyBuilder {
    type Prop = Schema;

    fn derive(
        &self,
        typ: OptRelNodeTyp,
        data: Option<optd_og_core::rel_node::Value>,
        children: &[&Self::Prop],
    ) -> Self::Prop {
        match typ {
            OptRelNodeTyp::Scan => {
                let name = data.unwrap().as_str().to_string();
                self.catalog.get(&name)
            }
            // ...
```

The schema property builder implements the `derive` function, which takes the plan node type, plan node data, and the children properties, in order to infer the property of the current plan node. The schema property is stored as a vector of data types in `Schema` structure. In optd_og, property will be type-erased and stored as `Box<dyn Any>` along with each `RelNode` group in the memo table. On the developer side, it does not need to handle all the type-erasing things and will work with typed APIs.

## Use a Property

When initializing an optimizer instance, developers will need to provide a vector of property builders to be computed. The property can then be retrieved using the index in the vector and the property builder type. For example, some optimizer rules will need to know the number of columns of a plan node before rewriting an expression.

For example, the current Datafusion optd_og optimizer is initialized with:

```rust
CascadesOptimizer::new_with_prop(
    rules,
    Box::new(cost_model),
    vec![Box::new(SchemaPropertyBuilder::new(catalog))],
    // ..
),
```

Therefore, developers can use index 0 and `SchemaPropertyBuilder` to retrieve the schema of a plan node after adding the node into the optimizer memo table.

```rust
impl PlanNode {
    pub fn schema(&self, optimizer: CascadesOptimizer<OptRelNodeTyp>) -> Schema {
        let group_id = optimizer.resolve_group_id(self.0.clone());
        optimizer.get_property_by_group::<SchemaPropertyBuilder>(group_id, 0 /* property ID */)
    }
}
```
