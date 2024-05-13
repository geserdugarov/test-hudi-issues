import os
from pathlib import Path

import pyspark

import utils


# Spark cluster connection settings
utils.set_spark_home()
spark = (pyspark.sql.SparkSession.builder
         .master("spark://linux-pc:7077")
         .appName("combine-before-insert")
         .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .getOrCreate())

# use current script name without .py as temporary directory name
tmp_dir_path = str(Path("/tmp") / os.path.basename(__file__)[:-3])
utils.prepare_temp_dirs(tmp_dir_path)

# prepare Spark DataFrame for further write
input_data = [pyspark.sql.Row(id=4, value="foo", ts=0),
              pyspark.sql.Row(id=4, value="bar", ts=1)]
df = spark.createDataFrame(input_data)

# Hudi configuration parameters
hudi_options = {
    "hoodie.table.name": "fake_name",
    "hoodie.datasource.write.table.name": "fake_name",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.metadata.enable": "false",
    "hoodie.bootstrap.index.enable": "false",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    # Testing out bulk insert
    "hoodie.combine.before.insert": "true",
    "hoodie.datasource.write.operation": "bulk_insert",
}

(df.write
 .format("org.apache.hudi")
 .options(**hudi_options)
 .mode("overwrite")
 .save(tmp_dir_path))

# Should be 1 but prints 2
print(spark.read.format("org.apache.hudi").load(tmp_dir_path).count())
# Both rows exist
print(spark.read.format("org.apache.hudi").load(tmp_dir_path).collect())
