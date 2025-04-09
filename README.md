# optd_og

optd_og (pronounced as op-dee) is a database optimizer framework. It is a cost-based optimizer that searches the plan space using the rules that the user defines and derives the optimal plan based on the cost model and the physical properties.

The primary objective of optd_og is to explore the potential challenges involved in effectively implementing a cost-based optimizer for real-world production usage. optd_og implements the Columbia Cascades optimizer framework based on [Yongwen Xu's master's thesis](https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/xu-columbia-thesis1998.pdf). Besides cascades, optd_og also provides a heuristics optimizer implementation for testing purpose.

The other key objective is to implement a flexible optimizer framework which supports adaptive query optimization (aka. reoptimization) and adaptive query execution. optd_og executes a query, captures runtime information, and utilizes this data to guide subsequent plan space searches and cost model estimations. This progressive optimization approach ensures that queries are continuously improved, and allows the optimizer to explore a large plan space.

Currently, optd_og is integrated into Apache Arrow Datafusion as a physical optimizer. It receives the logical plan from Datafusion, implements various physical optimizations (e.g., determining the join order), and subsequently converts it back into the Datafusion physical plan for execution.

optd_og is a research project and is still evolving. It should not be used in production. The code is licensed under MIT.

## Get Started

There are three demos you can run with optd_og. More information available in the [docs](docs/).

```
cargo run --release --bin optd_og-adaptive-tpch-q8
cargo run --release --bin optd_og-adaptive-three-join
```

You can also run the Datafusion cli to interactively experiment with optd_og.

```
cargo run --bin datafusion-optd_og-cli
```

You can also test the performance of the cost model with the "cardinality benchmarking" feature (more info in the [docs](docs/)).
Before running this, you will need to manually run Postgres on your machine.
Note that there is a CI script which tests this command (TPC-H with scale factor 0.01) before every merge into main, so it should be very reliable.
```
cargo run --release --bin optd_og-perfbench cardbench tpch --scale-factor 0.01
```

## Documentation

The documentation is available in the mdbook format in the [docs](docs) directory.

## Structure

* `datafusion-optd_og-cli`: The patched Apache Arrow Datafusion (version=32) cli that calls into optd_og.
* `datafusion-optd_og-bridge`: Implementation of Apache Arrow Datafusion query planner as a bridge between optd_og and Apache Arrow Datafusion.
* `optd_og-core`: The core framework of optd_og.
* `optd_og-datafusion-repr`: Representation of Apache Arrow Datafusion plan nodes in optd_og.
* `optd_og-adaptive-demo`: Demo of adaptive optimization capabilities of optd_og. More information available in the [docs](docs/).
* `optd_og-sqlplannertest`: Planner test of optd_og based on [risinglightdb/sqlplannertest-rs](https://github.com/risinglightdb/sqlplannertest-rs).
* `optd_og-gungnir`: Scalable, memory-efficient, and parallelizable statistical methods for cardinality estimation (e.g. TDigest, HyperLogLog).
* `optd_og-perfbench`: A CLI program for benchmarking performance (cardinality, throughput, etc.) against other databases.


# Related Works

* [datafusion-dolomite](https://github.com/datafusion-contrib/datafusion-dolomite)
