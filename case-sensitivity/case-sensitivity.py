import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp") / script_name)
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env(script_name)

spark.sql("SET spark.sql.caseSensitive = false")

spark.sql("CREATE TABLE case_sensitive ("
          "  AA string,"
          "  BB string,"
          "  PRECOMB long,"
          "  partition string"
          ") USING HUDI "
          "TBLPROPERTIES ("
          "  'primaryKey' = 'AA',"
          "  'type' = 'cow',"
          "  'preCombineField' = 'PRECOMB'"  # preCombineField is case-sensitive, and should be similar to defined in the schema 
          ") PARTITIONED BY ("
          "  partITION"
          ") LOCATION '" + tmp_dir_path + "';")
spark.sql("INSERT INTO case_sensitive VALUES "
          "  ('bx1', '11', 1, 'par1');")
spark.sql("INSERT INTO case_sensitive SELECT "
          " 'bx2' as aa, '22' as BB, 1 as preCOMB, 'par2' as PaRtItIoN;")

resultAll = spark.sql("SELECT * FROM case_sensitive;").collect()
resultFiltered = spark.sql("SELECT aa, BB, preCOMB, PaRtItIoN FROM case_sensitive;").collect()

print("  'SELECT *' results:")
for row in resultAll:
    print(row)
print("  'SELECT aa, BB, preCOMB, PaRtItIoN' results:")
for row in resultFiltered:
    print(row)
