# SQLPlannerTest

optd_og uses risinglightdb's SQL planner test library to ensure the optimizer works correctly and stably produces an expected plan. SQL planner test is a regression test. Developers provide the test framework a yaml file with the queries to be optimized and the information they want to collect. The test framework generates the test result and store them in SQL files. When a developer submits a pull request, the reviewers should check if any of these outputs are changed unexpectedly.

The test cases can be found in `optd_og-sqlplannertest/tests`. Currently, we check if optd_og can enumerate all join orders by using the `explain:logical_join_orders,physical_plan` task and check if the query output is as expected by using the `execute` task.
