import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp") / script_name)
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env_for_hudi(script_name)

spark.sql("CREATE TABLE cow_or_mor ("
          "  id int,"
          "  dt int"
          ") USING HUDI "
          "TBLPROPERTIES ("
          "  'primaryKey' = 'id',"
          "  'type' = 'cow',"
          "  'preCombineField' = 'dt',"
          "  'hoodie.index.type' = 'BUCKET',"
          "  'hoodie.index.bucket.engine' = 'SIMPLE',"
          "  'hoodie.bucket.index.num.buckets' = '2',"
          "  'hoodie.datasource.write.row.writer.enable' = 'false'"
          ") LOCATION '" + tmp_dir_path + "';")

spark.sql("SET hoodie.datasource.write.operation=bulk_insert")
spark.sql("INSERT INTO cow_or_mor VALUES (5, 10);")
spark.sql("INSERT INTO cow_or_mor VALUES (9, 30);")

resultAll = (spark.sql("SELECT * FROM cow_or_mor;").collect())
print("'SELECT *' results:")
for row in resultAll:
    print(row[2:])

# 'SELECT *' results:
# ('5', '', '00000000-dad4-4358-aaad-767a76e43e70-0_0-14-12_20241025153558259.parquet', 5, 10)
#
# We see only one row, and missed the second one with id=9, because it's placed in a log file, despite the fact that we set COW table
# tree -a /tmp/write-COW-get-MOR
# .
# ├── 00000000-dad4-4358-aaad-767a76e43e70-0_0-14-12_20241025153558259.parquet
# ├── .00000000-dad4-4358-aaad-767a76e43e70-0_0-14-12_20241025153558259.parquet.crc
# ├── .00000000-dad4-4358-aaad-767a76e43e70-0_20241025153606721.log.1_0-30-28
# ├── ..00000000-dad4-4358-aaad-767a76e43e70-0_20241025153606721.log.1_0-30-28.crc
