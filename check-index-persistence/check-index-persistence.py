import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp") / script_name)
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env_for_hudi(script_name)

spark.sql("CREATE TABLE index_persist ("
          "  id int,"
          "  dt string"
          ") USING HUDI "
          "TBLPROPERTIES ("
          "  'primaryKey' = 'id',"
          "  'type' = 'cow',"
          "  'preCombineField' = 'dt',"
          "  'hoodie.index.type' = 'BUCKET',"
          "  'hoodie.bucket.index.num.buckets' = '1'"
          ") LOCATION '" + tmp_dir_path + "';")

spark.sql("INSERT INTO index_persist VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0);")

spark.sql("SET hoodie.bucket.index.num.buckets=2")

spark.sql("INSERT INTO index_persist VALUES (1, 100), (2, 100), (3, 100), (4, 100), (5, 100);")

resultAll = (spark.sql("SELECT * FROM index_persist ORDER BY id;").collect())
print("'SELECT *' sorted results:")
for row in resultAll:
    print(row[2:])

# 'SELECT *' sorted results:
#     ('1', '', '00000000-0a6e-4a5d-af56-c804f7a69372-0_0-34-54_20250430181205708.parquet', 1, '100')
#     ('2', '', '00000000-0a6e-4a5d-af56-c804f7a69372-0_0-34-54_20250430181205708.parquet', 2, '0')
#     ('2', '', '00000001-62c8-49ba-9298-d0091a20f8e3-0_1-34-55_20250430181205708.parquet', 2, '100')
#     ('3', '', '00000000-0a6e-4a5d-af56-c804f7a69372-0_0-34-54_20250430181205708.parquet', 3, '100')
#     ('4', '', '00000000-0a6e-4a5d-af56-c804f7a69372-0_0-34-54_20250430181205708.parquet', 4, '0')
#     ('4', '', '00000001-62c8-49ba-9298-d0091a20f8e3-0_1-34-55_20250430181205708.parquet', 4, '100')
#     ('5', '', '00000000-0a6e-4a5d-af56-c804f7a69372-0_0-34-54_20250430181205708.parquet', 5, '100')

## We have duplications for records with ids 2 and 4, because there are old records in first bucket, and new records in second bucket.
