# The Gungnir™ team licenses this file to you under the MIT License (MIT);
# you may not use this file except in compliance with the License.
#
# Author: Alexis Schlomer <aschlome@andrew.cmu.edu>
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.

import os
import duckdb
import pyarrow.parquet as pq

SCALE_FACTOR = 1
DIR_NAME = "tpch_sf_" + str(SCALE_FACTOR)

os.makedirs(DIR_NAME, exist_ok=True)

# Use DuckDB to generate the TPCH data in memory.
con = duckdb.connect(database=':memory:')
con.execute("INSTALL tpch; LOAD tpch")
con.execute("CALL dbgen(sf = " + str(SCALE_FACTOR) + ")")
table_tuples = con.execute("show tables").fetchall()

# Dump from DuckDB to Parquet files.
for table_tuple in table_tuples:
    table_name = table_tuple[0]
    res = con.query("SELECT * FROM " + table_name)
    pq.write_table(res.to_arrow_table(), os.path.join(DIR_NAME, table_name + ".parquet"))