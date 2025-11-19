import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
spark = utils.init_spark_env_for_hudi(script_name)

# Hudi 0.14 should be used, which would prepare Hudi table version 6 in a result
list_of_payloads = [("event_time", "DefaultHoodieRecordPayload"),
                    ("overwrite_latest", "OverwriteWithLatestAvroPayload"),
                    ("custom", "PartialUpdateAvroPayload")]

for case, payload in list_of_payloads:
    tmp_dir_path = str(Path("/tmp") / ("MOR_" + case))
    utils.prepare_temp_dirs(tmp_dir_path=tmp_dir_path, create=True)
    table_name = "table_6_" + case

    spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " ("
              "    id int,"
              "    value string,"
              "    ts long,"
              "    for_partitions string"
              ") USING hudi"
              "  TBLPROPERTIES ("
              "    type = 'MERGE_ON_READ',"
              "    primaryKey = 'id',"
              "    preCombineField = 'ts',"
              "    hoodie.datasource.write.hive_style_partitioning = false,"
              "    hoodie.metadata.enable = false,"
              "    hoodie.compaction.payload.class = 'org.apache.hudi.common.model." + payload + "'"
              ") PARTITIONED BY (for_partitions)"
              "  LOCATION '" + tmp_dir_path + "'")

    spark.sql("INSERT INTO " + table_name + " VALUES (1, 'init_ts', 0, '0'), (2, 'init_ts', 1, '1')")
    spark.sql("INSERT INTO " + table_name + " VALUES (1, 'higher_ts', 1, '0')")
    spark.sql("INSERT INTO " + table_name + " VALUES (2, 'smaller_ts', 0, '1')")

    df_load = spark.sql("SELECT id, value, ts FROM " + table_name)
    print("For case '" + case + "' rows from SELECT are: ", df_load.collect())

# Next step is to run upgrade procedure
