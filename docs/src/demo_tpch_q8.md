# TPC-H Q8 Demo


You can run this demo with the following command:

```shell
cargo run --release --bin optd_og-adaptive-tpch-q8
```

In this demo, we create the TPC-H schema with test data of scale 0.01. There are 8 tables in TPC-H Q8, and it is impossible to enumerate all join combinations in one run. The demo will run this query multiple times, each time exploring a subset of the plan space. Therefore, optimization will be fast for each iteration, and as the plan space is more explored in each iteration, the produced plan will converge to the optimal join order.

```plain
--- ITERATION 5 ---
plan space size budget used, not applying logical rules any more. current plan space: 10354
(HashJoin region (HashJoin (HashJoin (HashJoin (HashJoin (HashJoin part (HashJoin supplier lineitem)) orders) customer) nation) nation))
plan space size budget used, not applying logical rules any more. current plan space: 11743
+--------+------------+
| col0   | col1       |
+--------+------------+
| 1995.0 | 1.00000000 |
| 1996.0 | 0.32989690 |
+--------+------------+
2 rows in set. Query took 0.115 seconds.
```

The output contains the current join order in Lisp representation, the plan space, and the query result.
