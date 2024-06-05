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
input_data = [pyspark.sql.Row(id=1, name="1,,,4", addr="abc,def,ghi", price=20),
              pyspark.sql.Row(id=2, name="5,5,5,5", addr="jkl,jkl,jkl", price=30)]
df = spark.createDataFrame(input_data)

# Hudi configuration parameters
hudi_options = {
    "hoodie.table.name": "table_name",
    "hoodie.datasource.write.table.name": "table_name",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.recordkey.field": "name,addr",
    "hoodie.datasource.write.precombine.field": "price",
    "hoodie.index.type": "BUCKET",
    "hoodie.bucket.index.num.buckets": "2"
}

(df.write
 .format("org.apache.hudi")
 .options(**hudi_options)
 .mode("overwrite")
 .save(tmp_dir_path))

df_load = spark.read.format("org.apache.hudi").load(tmp_dir_path)
print("Rows: ", df_load.select([col for col in df_load.columns if col in ['id', 'name', 'addr', 'price']]).collect())

# Fixed in Hudi 0.13, commit 3dc76af95d8e0c64c3dedfdd28c081046b905640
