# Summary

# optd_og book

[Intro to optd_og]()
- [The Core]()
  - [Plan Representation]()
  - [Memo Table and Logical Equivalence]()
  - [Cascades Framework]()
    - [Basic Cascades Tasks]()
    - [Cycle Avoidance]()
    - [Upper Bound Pruning]()
    - [Multi-Stage Optimization]()
  - [Rule IR and Matcher]()
  - [Cost and Statistics]()
  - [Logical Properties]()
  - [Physical Properties and Enforcers]()
    - [Memo Table: Subgoals and Winners]()
    - [Cascades Tasks: Required Physical Properties]()
  - [Exploration Budget]()
  - [Heuristics Optimizer]()
- [Integration with Datafusion]()
  - [Datafusion Plan Representation]()
  - [Datafusion Bridge]()
  - [Rule Engine and Rules]()
  - [Basic Cost Model]()
  - [Logical and Physical Properties]()
  - [Optimization Passes]()
  - [Miscellaneous]()
  - [Explain]()
- [Research]()
  - [Partial Exploration and Re-Optimization]()
  - [Advanced Cost Model]()
  - [The Hyper Subquery Unnesting Ruleset]()
- [Testing and Benchmark]()
  - [sqlplannertest]()
  - [sqllogictest]()
  - [perfbench]()
- [Debugging and Tracing]()
  - [optd_og-core Tracing]()
  - [Memo Table Visualization]()
  - [Optimizer Dump]()
- [Contribution Guide]()
  - [Install Tools]()
  - [Contribution Workflow]() 
  - [Add a Datafusion Rule]()
- [What's Next]()
  - [Ideas]()
  - [RFCs]()
---

# DEPRECATED
- [old optd_og book]()
  - [Core Framework]()
    - [Optimizer](./optimizer.md)
    - [Plan Representation](./plan_repr.md)
    - [Rule Engine](./rule_engine.md)
    - [Cost Model](./cost_model.md)
    - [Properties](./properties.md)
  - [Integration]()
    - [Apache Arrow Datafusion](./datafusion.md)
  - [Adaptive Optimization]()
    - [Re-optimization](./reoptimization.md)
    - [Partial Exploration](./partial_exploration.md)
  - [Demo]()
    - [Three Join Demo](./demo_three_join.md)
    - [TPC-H Q8 Demo](./demo_tpch_q8.md)
  - [Performance Benchmarking]()
    - [Cost Model Cardinality Benchmarking](./cost_model_benchmarking.md)
  - [Functional Testing]()
    - [SQLPlannerTest](./sqlplannertest.md)
    - [Datafusion CLI](./datafusion_cli.md)
  - [Miscellaneous](./miscellaneous.md)
