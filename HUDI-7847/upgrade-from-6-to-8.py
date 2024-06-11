import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
spark = utils.init_spark_env(script_name)

# Hudi 1.0 beta1 should be used to read, and upgrade previously prepared tables
list_of_merge_modes = [("event_time", "EVENT_TIME_ORDERING"),
                       ("overwrite_latest", "OVERWRITE_WITH_LATEST"),
                       ("custom", "CUSTOM")]

for case, merge_mode in list_of_merge_modes:
    tmp_dir_path = str(Path("/tmp") / ("MOR_" + case))

    spark.sql("CREATE TABLE IF NOT EXISTS table_6_" + case + " ("
              "    id int,"
              "    value string,"
              "    ts long,"
              "    for_partitions string"
              ") USING hudi"
              "  TBLPROPERTIES ("
              "    primaryKey = 'id',"
              "    preCombineField = 'ts'"
              ") LOCATION '" + tmp_dir_path + "'")

    spark.sql("call upgrade_table(table => 'table_6_" + case + "', to_version => 'EIGHT')")

    with open(tmp_dir_path + "/.hoodie/hoodie.properties", 'r') as file:
        lines = file.readlines()
        for line in lines:
            if line.startswith("hoodie.record.merge.mode"):
                # [:-1] is used to drop new line character
                print("In 'MOR_" + case + "' folder merge mode is '" + line.split("=")[1][:-1] + "'")
                break

# Expected output is:
#     In 'MOR_event_time' folder merge mode is 'EVENT_TIME_ORDERING'
#     In 'MOR_overwrite_latest' folder merge mode is 'OVERWRITE_WITH_LATEST'
#     In 'MOR_custom' folder merge mode is 'CUSTOM'
