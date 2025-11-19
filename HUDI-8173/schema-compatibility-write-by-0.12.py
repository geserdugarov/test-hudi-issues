import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp/schema_compatibility"))
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env_for_hudi(script_name)

spark.sql("CREATE TABLE schema_compatibility ("
          "  id long,"
          "  precomb long,"
          "  desc string,"
          "  partition string"
          ") USING HUDI "
          "TBLPROPERTIES ("
          "  'primaryKey' = 'id',"
          "  'type' = 'cow',"
          "  'preCombineField' = 'precomb'"
          ") PARTITIONED BY ("
          "  partition"
          ") LOCATION '" + tmp_dir_path + "';")

spark.sql("INSERT INTO schema_compatibility VALUES (1, 11, 'first row', 'par1');")
spark.sql("INSERT INTO schema_compatibility SELECT 1 as id, 22 as precomb, 'second row' as desc, 'par1' as partition;")
