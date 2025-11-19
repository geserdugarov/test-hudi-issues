import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp") / script_name)
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env_for_hudi(script_name)

spark.sql("CREATE TABLE cdc_check ("
          "  id long,"
          "  precomb long,"
          "  desc string,"
          "  partition string"
          ") USING HUDI "
          "TBLPROPERTIES ("
          "  'primaryKey' = 'id',"
          "  'type' = 'cow',"
          "  'preCombineField' = 'precomb',"
          "  'hoodie.table.cdc.enabled' = 'true',"
          "  'hoodie.table.cdc.supplemental.logging.mode' = 'DATA_BEFORE_AFTER'"
          ") PARTITIONED BY ("
          "  partition"
          ") LOCATION '" + tmp_dir_path + "';")

spark.sql("INSERT INTO cdc_check VALUES (1, 11, 'first row', 'par1');")
spark.sql("INSERT INTO cdc_check SELECT 1 as id, 22 as precomb, 'second row' as desc, 'par1' as partition;")

# load Hudi table
df_load = (spark.read
           .option("hoodie.datasource.read.begin.instanttime", 0)
           .option("hoodie.datasource.query.type", "incremental")
           .option("hoodie.datasource.query.incremental.format", "cdc")
           .format("org.apache.hudi")
           .load(tmp_dir_path))
for row in df_load.collect():
    print(row)
