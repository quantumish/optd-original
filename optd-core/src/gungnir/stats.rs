//----------------------------------------------------//
//   This free (MIT) Software is provided to you by   //
//       _____                         _              //
//      / ____|                       (_)             //
//      | |  __ _   _ _ __   __ _ _ __  _ _ __        //
//      | | |_ | | | | '_ \ / _` | '_ \| | '__|       //
//      | |__| | |_| | | | | (_| | | | | | |          //
//       \_____|\__,_|_| |_|\__, |_| |_|_|_|          //
//                           __/ |                    //
//                          |___/                     //
//                                                    //
// Author: Alexis Schlomer <aschlome@andrew.cmu.edu>  //
//----------------------------------------------------//

use arrow::array::{Array, Int32Array};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{fs::File, time::Instant};
use rand::distributions::{Distribution, Uniform};

use super::tdigest::TDigest;

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
                DataType::Float64 => {
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

// TODO(Alexis): Pure testing playground for now.
pub fn t_digest() {
    let mut t_digest = TDigest::new(100.0);

    let between = Uniform::new(-1000.0, 1.0);
    let mut rng = rand::thread_rng();

    let num_vectors = 10000;
    let vector_size = 1024;


    let start_time = Instant::now();

    for _ in 0..num_vectors {
        // Create a new vector
        let mut new_vector = vec![0.0; vector_size];

        // Fill the vector with random values from the uniform distribution
        for elem in new_vector.iter_mut() {
            *elem = between.sample(&mut rng);
        }

        // Add the new vector to the result vector
        t_digest = t_digest.merge_values(&mut new_vector);
        // println!("{:#?}", t_digest);
    }

    let elapsed_time = start_time.elapsed();


    println!("0.0: {:#?}", t_digest.quantile(0.0));
    println!("0.001: {:#?}", t_digest.quantile(0.001));
    println!("0.01: {:#?}", t_digest.quantile(0.01));
    println!("0.10: {:#?}", t_digest.quantile(0.10));
    println!("0.50: {:#?}", t_digest.quantile(0.50));
    println!("0.90: {:#?}", t_digest.quantile(0.90));
    println!("0.99: {:#?}", t_digest.quantile(0.99));
    println!("1.0: {:#?}", t_digest.quantile(1.0));

    println!("Elapsed time: {:?}", elapsed_time);
}