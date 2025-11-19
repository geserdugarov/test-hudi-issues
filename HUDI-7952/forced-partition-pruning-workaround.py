import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp") / script_name)
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env_for_hudi(script_name)

spark.sql("CREATE TABLE ts_partition_pruning ("
          "  id int,"
          "  name string,"
          "  precomb long,"
          "  ts long"
          ") USING HUDI "
          "TBLPROPERTIES ("
          "  'type' = 'cow',"
          "  'primaryKey' = 'id',"
          "  'preCombineField' = 'precomb',"
          "  'hoodie.datasource.write.partitionpath.field' = 'ts',"
          "  'hoodie.datasource.write.hive_style_partitioning' = 'false',"
          "  'hoodie.table.keygenerator.class' = 'org.apache.hudi.keygen.TimestampBasedKeyGenerator',"
          "  'hoodie.keygen.timebased.timestamp.type' = 'UNIX_TIMESTAMP',"
          "  'hoodie.keygen.timebased.output.dateformat' = 'yyyy-MM-dd HH'"
          ") LOCATION '" + tmp_dir_path + "';")

spark.sql("INSERT INTO ts_partition_pruning VALUES (1, 'a1', 1, 1078016523), (2, 'a2', 1, 1718952603);")
spark.sql("INSERT INTO ts_partition_pruning VALUES (2, 'a3', 1, 1718952603);")

resultAll = (spark.sql("SELECT id, name, precomb, ts FROM ts_partition_pruning;").collect())
print("SELECT all results:")
for row in resultAll:
    print(row)
# SELECT all results:
# Row(id=2, name='a3', precomb=1, ts='1718952603')
# Row(id=1, name='a1', precomb=1, ts='1078016523')

sameResultWithWideFilter = (spark.sql("SELECT * FROM ts_partition_pruning WHERE ts BETWEEN 1 and 9223372036854775807;").collect())
print("SELECT with huge range filter results:")
for row in sameResultWithWideFilter:
    print(row)
# SELECT with huge range filter results:
#

## Empty results in the second case, which is wrong
