import os
import shutil
from pathlib import Path

import dotenv
import pyspark


curr_dir = Path(__file__).parent.parent.absolute()
spark_home = dotenv.dotenv_values(str(curr_dir / ".env"))['SPARK_HOME']
os.environ['SPARK_HOME'] = str(spark_home)

# prepare test related temporary directory
tmp_dir_path = "/tmp/combine-before-insert"
if os.path.exists(tmp_dir_path):
    shutil.rmtree(tmp_dir_path)

# Settings
spark = (pyspark.sql.SparkSession.builder
         .master("spark://linux-pc:7077")
         .appName("combine-before-insert")
         .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         # .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
         # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
         # .config("spark.sql.hive.convertMetastoreParquet", "false")
         .getOrCreate())

# Data
input_data = [pyspark.sql.Row(id=4, value="foo", ts=0),
              pyspark.sql.Row(id=4, value="bar", ts=1)]
df = spark.createDataFrame(input_data)

# # Example Hudi configs
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
print(spark.read.format("hudi").load(tmp_dir_path).count())
# Both rows exist
print(spark.read.format("hudi").load(tmp_dir_path).collect())
