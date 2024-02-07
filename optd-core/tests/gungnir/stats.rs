// The Gungnir™ team licenses this file to you under the MIT License (MIT);
// you may not use this file except in compliance with the License.
//
// Author: Alexis Schlomer <aschlome@andrew.cmu.edu>
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.

use optd_core::gungnir::stats::compute_stats;
use optd_core::gungnir::stats::t_digest;

#[test]
fn run() {
    // Just an access point... Generate benchmark with python script!
    compute_stats("optd-core/tests/gungnir/tpch_sf_1/customer.parquet");
    t_digest();
}