import os
from pathlib import Path

import pyspark

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
spark = utils.init_spark_env(script_name)

# Hudi 0.14 should be used, which would prepare Hudi table version 6 in a result
list_of_payloads = [("event_time", "DefaultHoodieRecordPayload"),
                    ("overwrite_latest", "OverwriteWithLatestAvroPayload"),
                    ("custom", "PartialUpdateAvroPayload")]

for case, payload in list_of_payloads:
    tmp_dir_path = str(Path("/tmp") / ("MOR_" + case))
    utils.prepare_temp_dirs(tmp_dir_path=tmp_dir_path, create=True)

    spark.sql("CREATE TABLE IF NOT EXISTS table_6_" + case + " ("
              "    id int,"
              "    value string,"
              "    ts long,"
              "    for_partitions string"
              ") using hudi"
              "  tblproperties ("
              "    type = 'MERGE_ON_READ',"
              "    primaryKey = 'id',"
              "    preCombineField = 'ts',"
              "    hoodie.datasource.write.hive_style_partitioning = false,"
              "    hoodie.compaction.payload.class = 'org.apache.hudi.common.model." + payload + "'"
              ") partitioned by (for_partitions)"
              "  location '" + tmp_dir_path + "'")

    spark.sql("INSERT INTO table_6_" + case + " VALUES (1, 'foo', 0, '0'), (2, 'bar', 1, '1')")

# Next step is to run upgrade procedure
