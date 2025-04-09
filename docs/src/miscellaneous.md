# Miscellaneous

This is a note covering things that do not work well in the system right now.

## Type System

Currently, we hard code decimal type to have `15, 2` precision. Type inferences should be done in the schema property inference.

## Expression

optd_og supports exploring SQL expressions in the optimization process. However, this might be super inefficient as optimizing a plan node (i.e., join to hash join) usually needs the full binding of an expression tree. This could have exponential plan space and is super inefficient.

## Bindings

We do not have something like a binding iterator as in the Cascades paper. Before applying a rule, we will generate all bindings of a group, which might take a lot of memory. This should be fixed in the future.

## Cycle Detection + DAG

Consider the case for join commute rule.

```
(Join A B) <- group 1
(Projection (Join B A) <expressions list>) <- group 2
(Projection (Projection (Join A B) <expressions list>) <expressions list>) <- group 1 may refer itself
```

After applying the rule twice, the memo table will have self-referential groups. Currently, we detect such self-referential things in optimize group task. Probably there will be better ways to do that.

The same applies to DAG / Recursive CTEs -- we did not test if the framework works with DAG but in theory it should support it. We just need to ensure a node in DAG does not get searched twice.

# DAG

For DAG, another challenge is to recover the reusable fragments from the optimizer output. The optimizer can give you a DAG output but by iterating through the plan, you cannot know which parts can be reused/materialized. Therefore, we might need to produce some extra information with the plan node output. i.e., a graph-representation with metadata of each node, instead of `RelNode`. This also helps the process of inserting the physical collector plan nodes, which is currently a little bit hacky in the implementation.

## Memo Table

Obviously, it is not efficient to simply store a mapping from RelNode to the expression id. Cannot imagine how many levels of depths will it require to compute a hash of a tree structure.

## Partial Exploration

Each iteration will only be slower because we have to invoke the optimize group tasks before we can find a group to apply the rule. Probably we can keep the task stack across runs to make it faster.

## Physical Property + Enforcer Rules

A major missing feature in the optimizer. Need this to support shuffling and sort optimizations.

## Pruning

Currently, we have implemented the pruning condition as in the paper, but we did not actually enable it.
