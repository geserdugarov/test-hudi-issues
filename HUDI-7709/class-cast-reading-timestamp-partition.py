import os
from pathlib import Path

import pyspark

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp") / script_name)
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env_for_hudi(script_name)

# prepare Spark DataFrame for further write
input_data = [pyspark.sql.Row(id=1, value="foo", ts=1713033644000),
              pyspark.sql.Row(id=2, value="bar", ts=1713033887000),
              pyspark.sql.Row(id=3, value="baz", ts=1713033890000)]
df = spark.createDataFrame(input_data)

hudi_options = {
    # common settings
    "hoodie.table.name": "fake_name",
    "hoodie.datasource.write.table.name": "fake_name",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.metadata.enable": "false",
    "hoodie.bootstrap.index.enable": "false",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    # test related settings
    "hoodie.datasource.write.partitionpath.field": "ts",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
    "hoodie.keygen.timebased.output.dateformat": "yyyyMMddHH",
    "hoodie.keygen.timebased.timestamp.type": "EPOCHMILLISECONDS"
}

(df.write
 .format("org.apache.hudi")
 .options(**hudi_options)
 .mode("overwrite")
 .save(tmp_dir_path))

df_load = spark.read.format("org.apache.hudi").options(**hudi_options).load(tmp_dir_path)
# java.lang.ClassCastException here
#   java.lang.Long cannot be cast to org.apache.spark.unsafe.types.UTF8String
print("# of rows: ", df_load.count())
print("Rows: ", df_load.collect())

# The issue is fixed by https://github.com/apache/hudi/pull/11501, which has been reverted by https://github.com/apache/hudi/pull/11586
# Couldn't reproduce mentioned NullPointerException, but made another fix, https://github.com/apache/hudi/pull/11615
