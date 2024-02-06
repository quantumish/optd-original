// The Gungnir™ team licenses this file to you under the MIT License (MIT);
// you may not use this file except in compliance with the License.
//
// Author: Alexis Schlomer <aschlome@andrew.cmu.edu>
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.

use arrow::array::{Array, Int32Array};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

/// Represents the statistics of a one-dimensional column for a table in memory.
/// NOTE: Subject to change if we use T-Digest.
struct StatsData<T: PartialOrd> {
    name: String,                // Table name || Attribute name.
    n_distinct: u32,             // Number of unique values in the column.
    most_common_vals: Vec<T>,    // i.e. MCV.
    most_common_freqs: Vec<f64>, // Associated frequency of MCV.
    histogram_bounds: Vec<T>,    // Assuming equi-height histrograms.
}

/// Enumeration of currently supported data types in Gungnir™.
pub enum Stats {
    Int(StatsData<i32>),
    Float(StatsData<f64>),
    String(StatsData<String>),
}

/// Returns the statistics over all columns of the Parquet file.
pub fn compute_stats(path: &str) -> Result<Vec<Stats>, Box<dyn std::error::Error>> {
    // Obtain a batch iterator over the Parquet file.
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;

    // Initialize an empty statistics array.
    let mut stats = Vec::new();

    // Iterate over all record batches.
    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;

        for column in batch.columns() {
            match column.data_type() {
                DataType::Int32 => {
                    // Cast in proper type and iterate over values.
                }
                _ => {
                    // Skip unsupported data types.
                }
            }
        }
    }

    Ok(stats)
}
