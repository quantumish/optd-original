// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::process::{Command, Stdio};

use assert_cmd::prelude::CommandCargoExt;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for tests
    let _ = env_logger::try_init();
}

// TODO: fix these later. They're commented out since they were broken when we first received the codebase.
// #[rstest]
// #[case::exec_from_commands(
//     ["--command", "select 1", "--format", "json", "-q"],
//     "[{\"Int64(1)\":1}]\n"
// )]
// #[case::exec_multiple_statements(
//     ["--command", "select 1; select 2;", "--format", "json", "-q"],
//     "[{\"Int64(1)\":1}]\n[{\"Int64(2)\":2}]\n"
// )]
// #[case::exec_from_files(
//     ["--file", "tests/data/sql.txt", "--format", "json", "-q"],
//     "[{\"Int64(1)\":1}]\n"
// )]
// #[case::set_batch_size(
//     ["--command", "show datafusion.execution.batch_size", "--format", "json", "-q", "-b", "1"],
//     "[{\"name\":\"datafusion.execution.batch_size\",\"value\":\"1\"}]\n"
// )]
// #[test]
// fn cli_quick_test<'a>(#[case] args: impl IntoIterator<Item = &'a str>, #[case] expected: &str) {
//     let mut cmd = Command::cargo_bin("datafusion-optd_og-cli").unwrap();
//     cmd.args(args);
//     cmd.assert().stdout(predicate::eq(expected));
// }

#[test]
fn cli_test_tpch() {
    let mut cmd = Command::cargo_bin("datafusion-optd_og-cli").unwrap();
    cmd.current_dir(".."); // all paths in `test.sql` assume we're in the base dir of the repo
    cmd.args(["--file", "datafusion-optd_og-cli/tpch-sf0_01/test.sql"]);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let status = cmd.status().unwrap();
    assert!(
        status.success(),
        "should not have crashed when running tpch"
    );
}
