import os
from pathlib import Path

import pyspark

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
tmp_dir_path = str(Path("/tmp") / script_name)
utils.prepare_temp_dirs(tmp_dir_path)
spark = utils.init_spark_env(script_name)

# prepare Spark DataFrame for further write
input_data = [pyspark.sql.Row(id="4,:^龥且", value="foo", ts=0),
              pyspark.sql.Row(id="5,:Õö±•$", value="bar", ts=1)]
df = spark.createDataFrame(input_data)

# Hudi configuration parameters
hudi_options = {
    "hoodie.table.name": "table_name",
    "hoodie.datasource.write.table.name": "table_name",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts"
}

(df.write
 .format("org.apache.hudi")
 .options(**hudi_options)
 .mode("overwrite")
 .save(tmp_dir_path))

df_load = spark.read.format("org.apache.hudi").load(tmp_dir_path)
# Only one row is expected
print("# of rows: ", df_load.count())
print("Rows: ", df_load.select([col for col in df_load.columns if col in ['id', 'value', 'ts']]).collect())

# For master branch everything is ok, but for 0.15 there is NullPointerException
