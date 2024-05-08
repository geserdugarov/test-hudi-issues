from pyspark.sql import Row
from pyspark.sql import SparkSession

# Settings
spark = SparkSession.builder.appName("combine-before-insert").getOrCreate()

# Data
input_data = [
    Row(
        id=4,
        value="foo",
        ts=0,
    ),
    Row(
        id=4,
        value="bar",
        ts=1,
    ),
]
df = spark.createDataFrame(input_data)

# Example Hudi configs
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

PATH = "/tmp/combine-before-insert"
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(PATH)

# Should be 1 but prints 2
print(pyspark.read.format("hudi").load(PATH).count())
# Both rows exist
print(pyspark.read.format("hudi").load(PATH).collect())
