// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use serde::{Deserialize, Serialize};

use crate::job::JobKitConfig;
use crate::tpch::TpchKitConfig;

#[derive(Deserialize, Serialize)]
pub enum Benchmark {
    Tpch(TpchKitConfig),
    Job(JobKitConfig),
    Joblight(JobKitConfig),
}

impl Benchmark {
    /// Get a string that deterministically describes the "data" and "queries" of this benchmark.
    pub fn get_data_and_queries_name(&self) -> String {
        match self {
            Self::Tpch(tpch_kit_config) => {
                format!("tpch_sf{}", tpch_kit_config.scale_factor)
            }
            Self::Job(_) => String::from("job"),
            Self::Joblight(_) => String::from("joblight"),
        }
    }

    /// Use this when you need a file name to deterministically describe the "data"
    ///   of the benchmark. The rules for file names are different from the rules for
    ///   database names, so this is a different function.
    pub fn get_fname(&self) -> String {
        match self {
            Self::Tpch(tpch_kit_config) => {
                format!("tpch_sf{}", tpch_kit_config.scale_factor)
            }
            Self::Job(_) | Self::Joblight(_) => String::from("job"),
        }
    }

    /// Get a dbname that deterministically describes the "data" of this benchmark.
    /// Note that benchmarks consist of "data" and "queries". This name is only for the data
    /// For instance, if you have two TPC-H benchmarks with the same scale factor and seed
    ///   but different queries, they could both share the same database and would thus
    ///   have the same dbname.
    /// This name must be compatible with the rules all databases have for their names, which
    ///   are described below:
    ///
    /// Postgres' rules:
    ///   - The name can only contain A-Z a-z 0-9 _ and cannot start with 0-9.
    ///   - There is a weird behavior where if you use CREATE DATABASE to create a database,
    ///     Postgres will convert uppercase letters to lowercase. However, if you use psql to then
    ///     connect to the database, Postgres will *not* convert capital letters to lowercase. To
    ///     resolve the inconsistency, the names output by this function will *not* contain
    ///     uppercase letters.
    pub fn get_dbname(&self) -> String {
        let fname = self.get_fname();
        // since Postgres names cannot contain periods
        let dbname = fname.replace('.', "point");
        // due to the weird inconsistency with Postgres (see function comment)
        dbname.to_lowercase()
    }

    pub fn is_readonly(&self) -> bool {
        match self {
            Self::Tpch(_) => true,
            Self::Job(_) => true,
            Self::Joblight(_) => true,
        }
    }
}
