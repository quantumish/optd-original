# Three Join Demo

You can run this demo with the following command:

```shell
cargo run --release --bin optd_og-adaptive-three-join
```

We create 3 tables and join them. The underlying data are getting updated every time the query is executed.

```sql
select * from t1, t2, t3 where t1v1 = t2v1 and t1v2 = t3v2;
```

When the data distribution and the table size changes, the optimal join order will be different. The output of this demo is as below.

```plain
Iter  66: (HashJoin (HashJoin t1 t2) t3) <-> (best) (HashJoin (HashJoin t1 t2) t3), Accuracy: 66/66=100.000
Iter  67: (HashJoin (HashJoin t2 t1) t3) <-> (best) (HashJoin (HashJoin t1 t2) t3), Accuracy: 66/67=98.507
Iter  68: (HashJoin t2 (HashJoin t1 t3)) <-> (best) (HashJoin (HashJoin t1 t2) t3), Accuracy: 66/68=97.059
Iter  69: (HashJoin (HashJoin t1 t2) t3) <-> (best) (HashJoin (HashJoin t1 t2) t3), Accuracy: 67/69=97.101
Iter  70: (HashJoin (HashJoin t1 t2) t3) <-> (best) (HashJoin (HashJoin t1 t2) t3), Accuracy: 68/70=97.143
Iter  71: (HashJoin (HashJoin t1 t2) t3) <-> (best) (HashJoin (HashJoin t1 t2) t3), Accuracy: 69/71=97.183
Iter  72: (HashJoin (HashJoin t2 t1) t3) <-> (best) (HashJoin (HashJoin t1 t2) t3), Accuracy: 69/72=95.833
```

The left plan Lisp representation is the join order determined by the adaptive query optimization algorithm. The right plan is the best plan. The accuracy is the percentage of executions that the adaptive query optimization algorithm generates the best cost-optimal plan.

To find the optimal plan and compute the accuracy, we set up two optimizers in this demo: the normal optimizer and the optimal optimizer. Each time we insert some data into the tables, we will invoke the normal optimizer once, and invoke the optimal optimizer with all possible combination of join orders, so that the optimal optimizer can produce an optimal plan based on the cost model and the join selectivity.

As the algorithm can only know the runtime information from last run before new data are added into the tables, there will be some iterations where it cannot generate the optimal plan. But it will converge to the optimal plan as more runtime information is collected.
