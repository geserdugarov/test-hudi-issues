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
input_data = [pyspark.sql.Row(id=1, name="a1", precomb=1),
              pyspark.sql.Row(id=2, name="a2", precomb=1)]
df = spark.createDataFrame(input_data)

# Hudi configuration parameters
hudi_options = {
    "hoodie.table.name": "table_name",
    "hoodie.datasource.write.table.name": "table_name",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "precomb"
}

(df.write
 .format("org.apache.hudi")
 .options(**hudi_options)
 .mode("overwrite")
 .save(tmp_dir_path))

df_load = spark.read.format("org.apache.hudi").load(tmp_dir_path)
print("Finished loading, started to collect")
print("Rows: ", df_load.collect())

# Fixed by https://github.com/apache/hudi/pull/11626

# Previously got java.lang.NullPointerException if spark.kryo.registrator isn't passed
#     at org.apache.hadoop.conf.Configuration.<init>(Configuration.java:842)
#     at org.apache.hudi.storage.hadoop.HadoopStorageConfiguration.unwrapCopy(HadoopStorageConfiguration.java:73)
#     at org.apache.hudi.storage.hadoop.HadoopStorageConfiguration.unwrapCopy(HadoopStorageConfiguration.java:36)
#     at org.apache.spark.sql.execution.datasources.parquet.SparkParquetReaderBase.read(SparkParquetReaderBase.scala:58)
#     at org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedParquetFileFormat.$anonfun$buildReaderWithPartitionValues$3(HoodieFileGroupReaderBasedParquetFileFormat.scala:193)
#     at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1.org$apache$spark$sql$execution$datasources$FileScanRDD$$anon$$readCurrentFile(FileScanRDD.scala:231)
#     at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1.nextIterator(FileScanRDD.scala:293)
#     at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1.hasNext(FileScanRDD.scala:125)
#     at org.apache.spark.sql.execution.FileSourceScanExec$$anon$1.hasNext(DataSourceScanExec.scala:594)
#     at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.columnartorow_nextBatch_0$(Unknown Source)
#     at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
#     at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
#     ...
