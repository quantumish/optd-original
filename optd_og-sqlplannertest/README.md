# SQL Planner Tests

These test cases use the [sqlplannertest](https://crates.io/crates/sqlplannertest) crate to execute SQL queries and inspect their output.
They do not check whether a plan is correct, and instead rely on a text-based diff of the query's output to determine whether something is different than the expected output.

We are also using this crate to generate benchmarks for evaluating optd_og's performance. With the help with the [criterion](https://crates.io/crates/criterion) crate, we can benchmark planning time and the execution time of physical plan produced by the optimizer.

## Execute Test Cases

**Running all test cases**

```shell
cargo test -p optd_og-sqlplannertest
# or use nextest
cargo nextest run -p optd_og-sqlplannertest
```

**Running tests in specfic modules or files**

```shell
# Running all test cases in the tpch module
cargo nextest run -p optd_og-sqlplannertest tpch
# Running all test cases in the tests/subqueries/subquery_unnesting.yml
cargo nextest run -p optd_og-sqlplannertest subquery::subquery_unnesting
```

## Executing Benchmarks

There are two metrics we care about when evaluating 

### Usage

```shell
# Benchmark all TPC-H queries with "bench" task enabled
cargo bench --bench planner_bench tpch/

# Benchmark TPC-H Q1
cargo bench --bench planner_bench tpch/q1/

# Benchmark TPC-H Q1 planning
cargo bench --bench planner_bench tpch/q1/planning

# Benchmark TPC-H Q1 execution
cargo bench --bench planner_bench tpch/q1/execution

# View the HTML report
python3 -m http.server -d ./target/criterion/
```

### Limitations

`planner_bench` can only handle `sqlplannertest` yaml-based test file with single test case.


## Add New Test Case

To add a SQL query tests, create a YAML file in a subdir in "tests".
Each file can contain multiple tests that are executed in sequential order from top to bottom.

```yaml
- sql: |
    CREATE TABLE xxx (a INTEGER, b INTEGER);
    INSERT INTO xxx VALUES (0, 0), (1, 1), (2, 2);
  tasks:
    - execute
  desc: Database Setup
- sql: |
    SELECT * FROM xxx WHERE a = 0;
  tasks:
    - execute
    - explain:logical_optd_og,physical_optd_og
  desc: Equality predicate
```
| Name    | Description                                                   |
| ------- | ------------------------------------------------------------- |
| `sql`   | List of SQL statements to execute separate by newlines        |
| `tasks` | How to execute the SQL statements. See [Tasks](#tasks) below  |
| `desc`  | (Optional) Text description of what the test cases represents |

After adding the YAML file, you then need to use the update command to automatically create the matching SQL file that contains the expected output of the test cases.

## Regenerate Test Case Output

If you change the output of optd_og's `EXPLAIN` syntax, then you need to update all of the expected output files for each test cases.
The following commands will automatically update all of them for you. You should try to avoid this doing this often since if you introduce a bug, all the outputs will get overwritten and the tests will report false negatives.

```shell
# Update all test cases
cargo run -p optd_og-sqlplannertest --bin planner_test_apply
# or, supply a list of modules or files to update
cargo run -p optd_og-sqlplannertest --bin planner_test_apply -- subqueries tpch::q1
```

## Tasks

The `explain` and `execute` task will be run with datafusion's logical optimizer disabled. Each task has some toggleable flags to control its behavior.

The `bench` task is only used in benchmarks. A test case can only be executed as a benchmark if a bench task exists.

### `execute` Task

#### Flags

| Name             | Description                           |
| ---------------- | ------------------------------------- |
| `use_df_logical` | Enable Datafusion's logical optimizer |

### Explain Task

#### Flags

| Name             | Description                                                        |
| ---------------- | ------------------------------------------------------------------ |
| `use_df_logical` | Enable Datafusion's logical optimizer                              |
| `verbose`        | Display estimated cost in physical plan                            |
| `logical_rules`  | Only enable these logical rules (also disable heuristic optimizer) |

Currently we have the following options for the explain task:

- `logical_datafusion`: datafusion's logical plan.
- `logical_optd_og`: optd_og's logical plan before optimization.
- `optimized_logical_optd_og`: optd_og's logical plan after heuristics optimization and before cascades optimization.
- `physical_optd_og`: optd_og's physical plan after optimization.
- `physical_datafusion`: datafusion's physical plan.
- `join_orders`: physical join orders.
- `logical_join_orders`: logical join orders.

## Tracing a query

```
RUST_BACKTRACE=1 RUST_LOG=optd_og_core=trace,optd_og_datafusion_bridge=trace cargo run -p optd_og-sqlplannertest --bin planner_test_apply -- pushdowns &> log
```
