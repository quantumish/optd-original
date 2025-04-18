// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

pub mod bench_helper;

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::catalog::CatalogProviderList;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion_optd_og_cli::helper::unescape_input;
use itertools::Itertools;
use lazy_static::lazy_static;
use mimalloc::MiMalloc;
use optd_og_datafusion_bridge::{create_df_context, OptdDfContext, OptdQueryPlanner};
use regex::Regex;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::{bail, Result};
use async_trait::async_trait;

#[derive(Default)]
pub struct DatafusionDBMS {
    ctx: SessionContext,
    /// Context enabling datafusion's logical optimizer.
    use_df_logical_ctx: SessionContext,
    /// Shared optd_og optimizer (for tweaking config)
    optd_og_optimizer: Option<Arc<OptdQueryPlanner>>,
}

impl DatafusionDBMS {
    pub async fn new() -> Result<Self> {
        let (ctx, optd_og_optimizer) = DatafusionDBMS::new_session_ctx(false, None, false).await?;
        let (use_df_logical_ctx, _) =
            Self::new_session_ctx(true, Some(ctx.state().catalog_list().clone()), false).await?;
        Ok(Self {
            ctx,
            use_df_logical_ctx,
            optd_og_optimizer: Some(optd_og_optimizer),
        })
    }

    pub async fn new_advanced_cost() -> Result<Self> {
        let (ctx, optd_og_optimizer) = DatafusionDBMS::new_session_ctx(false, None, true).await?;
        let (use_df_logical_ctx, _) =
            Self::new_session_ctx(true, Some(ctx.state().catalog_list().clone()), true).await?;
        Ok(Self {
            ctx,
            use_df_logical_ctx,
            optd_og_optimizer: Some(optd_og_optimizer),
        })
    }

    /// Creates a new session context. If the `use_df_logical` flag is set, datafusion's logical
    /// optimizer will be used.
    async fn new_session_ctx(
        use_df_logical: bool,
        catalog: Option<Arc<dyn CatalogProviderList>>,
        with_advanced_cost: bool,
    ) -> Result<(SessionContext, Arc<OptdQueryPlanner>)> {
        let OptdDfContext { ctx, optimizer, .. } = create_df_context(
            None,
            None,
            catalog,
            false,
            use_df_logical,
            with_advanced_cost,
            None,
        )
        .await?;
        Ok((ctx, optimizer))
    }

    /// Sets up test specific behaviors based on `flags`.
    pub(crate) async fn setup(&self, flags: &TestFlags) -> Result<()> {
        let mut guard = self
            .optd_og_optimizer
            .as_ref()
            .unwrap()
            .optimizer
            .lock()
            .unwrap();
        let optimizer = guard.as_mut().unwrap().optd_og_optimizer_mut();

        optimizer.prop.panic_on_budget = flags.panic_on_budget;
        optimizer.prop.enable_tracing = flags.enable_tracing;
        optimizer.prop.disable_pruning = flags.disable_pruning;
        let rules = optimizer.rules();
        if flags.enable_logical_rules.is_empty() {
            for r in 0..rules.len() {
                optimizer.enable_rule(r);
            }
            guard.as_mut().unwrap().enable_heuristic(true);
        } else {
            for (rule_id, rule) in rules.as_ref().iter().enumerate() {
                if rule.is_impl_rule() {
                    optimizer.enable_rule(rule_id);
                } else {
                    optimizer.disable_rule(rule_id);
                }
            }
            let mut rules_to_enable = flags
                .enable_logical_rules
                .iter()
                .map(|x| x.as_str())
                .collect::<HashSet<_>>();
            for (rule_id, rule) in rules.as_ref().iter().enumerate() {
                if rules_to_enable.remove(rule.name()) {
                    optimizer.enable_rule(rule_id);
                }
            }
            if !rules_to_enable.is_empty() {
                bail!("Unknown logical rule: {:?}", rules_to_enable);
            }
            guard.as_mut().unwrap().enable_heuristic(false);
        }

        Ok(())
    }

    /// Parses input SQL string into statements.
    pub async fn parse_sql(&self, sql: &str) -> Result<VecDeque<Statement>> {
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        Ok(statements)
    }

    /// Creates a physical execution plan with associated task context from a SQL statement.
    pub(crate) async fn create_physical_plan(
        &self,
        stmt: Statement,
        flags: &TestFlags,
    ) -> Result<(Arc<dyn ExecutionPlan>, Arc<TaskContext>)> {
        let df = if flags.enable_df_logical {
            let plan = self
                .use_df_logical_ctx
                .state()
                .statement_to_plan(stmt)
                .await?;
            self.use_df_logical_ctx.execute_logical_plan(plan).await?
        } else {
            let plan = self.ctx.state().statement_to_plan(stmt).await?;

            self.ctx.execute_logical_plan(plan).await?
        };
        let task_ctx = Arc::new(df.task_ctx());
        let plan = df.create_physical_plan().await?;
        Ok((plan, task_ctx))
    }

    /// Executes the physical [`ExecutionPlan`] and collect the results in memory.
    pub(crate) async fn execute_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = datafusion::physical_plan::collect(plan, task_ctx).await?;
        Ok(batches)
    }

    pub async fn execute(&self, sql: &str, flags: &TestFlags) -> Result<Vec<Vec<String>>> {
        self.setup(flags).await?;
        let statements = self.parse_sql(sql).await?;
        let mut result = Vec::new();
        for statement in statements {
            let (plan, task_ctx) = self.create_physical_plan(statement, flags).await?;
            let batches = self.execute_physical(plan, task_ctx).await?;
            let options = FormatOptions::default().with_null("NULL");

            for batch in batches {
                let converters = batch
                    .columns()
                    .iter()
                    .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
                    .collect::<Result<Vec<_>, _>>()?;
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::with_capacity(batch.num_columns());
                    for converter in converters.iter() {
                        let mut buffer = String::with_capacity(8);
                        converter.value(row_idx).write(&mut buffer)?;
                        row.push(buffer);
                    }
                    result.push(row);
                }
            }
        }
        Ok(result)
    }

    /// Executes the `execute` task.
    async fn task_execute(&mut self, r: &mut String, sql: &str, flags: &TestFlags) -> Result<()> {
        use std::fmt::Write;
        if flags.verbose {
            bail!("Verbose flag is not supported for execute task");
        }
        let result = self.execute(sql, flags).await?;
        writeln!(r, "{}", result.into_iter().map(|x| x.join(" ")).join("\n"))?;
        writeln!(r)?;
        Ok(())
    }

    /// Executes the `explain` task.
    async fn task_explain(
        &mut self,
        r: &mut String,
        sql: &str,
        task: &str,
        flags: &TestFlags,
    ) -> Result<()> {
        use std::fmt::Write;

        let verbose = flags.verbose;
        let explain_sql = if verbose {
            format!("explain verbose {}", &sql)
        } else {
            format!("explain {}", &sql)
        };
        let result = self.execute(&explain_sql, flags).await?;
        let subtask_start_pos = task.rfind(':').unwrap() + 1;
        for subtask in task[subtask_start_pos..].split(',') {
            let subtask = subtask.trim();
            if subtask == "logical_datafusion" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after datafusion")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "logical_optd_og_heuristic" || subtask == "optimized_logical_optd_og" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after optd_og-heuristic")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "logical_optd_og" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after optd_og")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "physical_optd_og" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan after optd_og")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "logical_join_orders" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan after optd_og-all-logical-join-orders")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
                writeln!(r)?;
            } else if subtask == "physical_datafusion" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else {
                bail!("Unknown subtask: {}", subtask);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl sqlplannertest::PlannerTestRunner for DatafusionDBMS {
    async fn run(&mut self, test_case: &sqlplannertest::ParsedTestCase) -> Result<String> {
        let mut result = String::new();
        let r = &mut result;
        for sql in &test_case.before_sql {
            // We drop output of before statements
            self.execute(sql, &TestFlags::default()).await?;
        }
        for task in &test_case.tasks {
            let flags = extract_flags(task)?;
            if task.starts_with("execute") {
                self.task_execute(r, &test_case.sql, &flags).await?;
            } else if task.starts_with("explain") {
                self.task_explain(r, &test_case.sql, task, &flags).await?;
            }
            if flags.dump_memo_table {
                let mut guard = self
                    .optd_og_optimizer
                    .as_ref()
                    .unwrap()
                    .optimizer
                    .lock()
                    .unwrap();
                let optimizer = guard.as_mut().unwrap().optd_og_optimizer_mut();
                let mut buf = String::new();
                optimizer.dump(&mut buf).unwrap();
                r.push_str(&buf);
            }
        }
        Ok(result)
    }
}

lazy_static! {
    static ref FLAGS_REGEX: Regex = Regex::new(r"\[(.*)\]").unwrap();
}

#[derive(Default, Debug)]
pub struct TestFlags {
    verbose: bool,
    enable_df_logical: bool,
    enable_logical_rules: Vec<String>,
    panic_on_budget: bool,
    enable_tracing: bool,
    dump_memo_table: bool,
    disable_pruning: bool,
}

/// Extract the flags from a task. The flags are specified in square brackets.
/// For example, the flags for the task `explain[use_df_logical, verbose]` are `["use_df_logical",
/// "verbose"]`.
pub fn extract_flags(task: &str) -> Result<TestFlags> {
    if let Some(captures) = FLAGS_REGEX.captures(task) {
        let flags = captures
            .get(1)
            .unwrap()
            .as_str()
            .split(',')
            .map(|x| x.trim().to_string())
            .collect_vec();
        let mut options = TestFlags::default();
        for flag in flags {
            if flag == "verbose" {
                options.verbose = true;
            } else if flag == "use_df_logical" {
                options.enable_df_logical = true;
            } else if flag.starts_with("logical_rules") {
                if let Some((_, flag)) = flag.split_once(':') {
                    options.enable_logical_rules = flag.split('+').map(|x| x.to_string()).collect();
                } else {
                    bail!("Failed to parse logical_rules flag: {}", flag);
                }
            } else if flag == "panic_on_budget" {
                options.panic_on_budget = true;
            } else if flag == "dump_memo_table" {
                options.dump_memo_table = true;
            } else if flag == "disable_pruning" {
                options.disable_pruning = true;
            } else if flag == "enable_tracing" {
                options.enable_tracing = true;
            } else {
                bail!("Unknown flag: {}", flag);
            }
        }
        Ok(options)
    } else {
        Ok(TestFlags::default())
    }
}
