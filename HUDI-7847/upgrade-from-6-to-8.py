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
    table_name = "table_6_" + case

    spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " ("
              "    id int,"
              "    value string,"
              "    ts long,"
              "    for_partitions string"
              ") USING hudi"
              "  TBLPROPERTIES ("
              "    primaryKey = 'id',"
              "    preCombineField = 'ts',"
              "    hoodie.metadata.enable = false"
              ") LOCATION '" + tmp_dir_path + "'")

    spark.sql("call upgrade_table(table => '" + table_name + "', to_version => 'EIGHT')")

    # After
    # https://github.com/apache/hudi/pull/9617
    # completion time is expected in the timeline.
    # So, I can only check changes in `hoodie.properties`
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


    # hudi_options = {
    #     "hoodie.table.name": table_name,
    #     "hoodie.metadata.enable": "false"
    # }
    # df_load = spark.read.format("org.apache.hudi").options(**hudi_options).load(tmp_dir_path)
    # print("For case '" + case + "' rows from SELECT are: ", df_load.collect())

    # Got
    # pyspark.errors.exceptions.captured.IllegalArgumentException: Completion time should not be empty


    # df_load = spark.sql("SELECT id, value, ts FROM " + table_name)
    # print("For case '" + case + "' rows from SELECT are: ", df_load.collect())

    # Got
    # py4j.protocol.Py4JJavaError: An error occurred while calling o37.collectToPython.
    # : org.apache.spark.SparkException: Job aborted due to stage failure: Task 1 in stage 5.0 failed 4 times, most recent failure: Lost task 1.3 in stage 5.0 (TID 16) (10.199.141.90 executor 0): java.lang.NullPointerException
    #     Caused by: java.lang.NullPointerException
    #     at org.apache.hudi.storage.hadoop.HadoopStorageConfiguration.getString(HadoopStorageConfiguration.java:83)
    #     at org.apache.hudi.storage.HoodieStorageUtils.getStorage(HoodieStorageUtils.java:38)
    #     at org.apache.hudi.common.table.HoodieTableMetaClient.getStorage(HoodieTableMetaClient.java:403)
    #     at org.apache.hudi.common.table.HoodieTableMetaClient.access$000(HoodieTableMetaClient.java:102)
    #     at org.apache.hudi.common.table.HoodieTableMetaClient$Builder.build(HoodieTableMetaClient.java:908)
