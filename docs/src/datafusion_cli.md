# Datafusion CLI

Developers can interact with optd_og by using the Datafusion cli. The cli supports creating tables, populating data, and executing ANSI SQL queries.

```shell
cargo run --bin datafusion-optd_og-cli
```

We also have a scale 0.01 TPC-H dataset to test. The test SQL can be executed with the Datafusion cli.

```shell
cargo run --bin datafusion-optd_og-cli -- -f datafusion-optd_og-cli/tpch-sf0_01/test.sql
```
