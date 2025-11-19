import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp") / script_name)
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env_for_hudi(script_name)

spark.sql("SET spark.sql.caseSensitive = false")

spark.sql("CREATE TABLE case_sensitive ("
          "  aa string,"
          "  BB string,"
          "  preCOMB long,"
          "  PaRtItIoN string"
          ") USING HUDI "
          "TBLPROPERTIES ("
          "  'primaryKey' = 'aa',"  # primaryKey is case-sensitive, and should be similar to defined in the schema
          "  'type' = 'cow',"
          "  'preCombineField' = 'preCOMB'"  # preCombineField is case-sensitive, and should be similar to defined in the schema 
          ") PARTITIONED BY ("
          "  partition"
          ") LOCATION '" + tmp_dir_path + "';")
spark.sql("INSERT INTO case_sensitive VALUES "
          "  ('bx1', '11', 1, 'par1');")
spark.sql("INSERT INTO case_sensitive SELECT "
          " 'bx2' as Aa, '22' as bB, 1 as PRECOMB, 'par2' as PARTITION;")

resultAll = (spark.sql("SELECT * FROM case_sensitive;").collect())
resultFiltered = spark.sql("SELECT aA, Bb, PrEcOmB, pArTiTiOn FROM case_sensitive;").collect()

print("  'SELECT *' results with dropped meta columns:")
for row in resultAll:
    print(row[2:])
print("  'SELECT aa, BB, preCOMB, PaRtItIoN' results:")
for row in resultFiltered:
    print(row)
