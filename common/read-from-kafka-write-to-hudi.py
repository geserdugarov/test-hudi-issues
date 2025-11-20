import os

import utils
from pyspark.sql import functions


def prepare_offset_config(kafka_topic: str, partitions_num: int, set_offset: int) -> str:
    config = f"""{{"{kafka_topic}": {{"""
    for partition_id in range(partitions_num-1):
        config += f""" "{partition_id}":{set_offset}, """
    config += f""" "{partitions_num-1}":{set_offset}}}}} """
    return config


# prepare environment
script_name = os.path.basename(__file__)[:-3]
spark = utils.init_spark_env_for_hudi(script_name)

# Kafka read configuration
kafka_brokers = "ip1:port1,ip2:port2"
kafka_topic = "tpch_60kk"
kafka_partitions_num = 8
batch_size_from_1_partition = 1_500_000
num_of_batches = 5

# Hudi configuration
# If HDFS is used, then `fs.default.name` should be set in Spark's ./conf/core-site.xml, as:
#   <property>
#       <name>fs.default.name</name>
#       <value>hdfs://hdfs-namenode-ip:9000</value>
#   </property>
# For local write, use file:///tmp/some-path
table_name = "kafka_to_hudi_mor_upsert"
hudi_table_path = f"hdfs://hdfs-namenode-ip:9000/<user>/hudi/{table_name}"
hudi_options = {
    "hoodie.table.name": f"{table_name}",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    # operation
    "hoodie.datasource.write.operation": "upsert",
    # partitioning
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    # keys
    "hoodie.datasource.write.recordkey.field": "l_orderkey,l_linenumber",
    "hoodie.datasource.write.precombine.field": "l_linenumber",
    # parquet configuration
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.logfile.data.block.format": "parquet",
    # table services configuration
    "hoodie.compact.inline": "false",
    "hoodie.compact.schedule.inline": "false",
    "hoodie.compact.inline.max.delta.commits": "50",
    "hoodie.clean.async.enabled": "false",
    # recommendations from Hudi docs
    # (input_data_size_MB / 120MB) = 16_000 / 120 ~= 128
    # input_data_size_MB ~= (~200 MB per 1 bucket and 1 commit) * 16 buckets * 5 commits
    "hoodie.bulkinsert.shuffle.parallelism": "128",
    "hoodie.insert.shuffle.parallelism": "128",
    "hoodie.upsert.shuffle.parallelism": "128",
    "hoodie.delete.shuffle.parallelism": "128",
    # lock provider
    "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    # bucket index case
    "hoodie.index.type": "BUCKET",
    "hoodie.bucket.index.hash.field": "l_orderkey,l_linenumber",
    "hoodie.bucket.index.num.buckets": "16",
    "hoodie.index.bucket.engine": "SIMPLE"
}

# data in Kafka topic is bytearray of csv string, this schema allows to parse this string
csv_schema = (
    "l_orderkey INT, "
    "l_partkey INT, "
    "l_suppkey INT, "
    "l_linenumber INT, "
    "l_quantity DECIMAL, "
    "l_extendedprice DECIMAL, "
    "l_discount DECIMAL, "
    "l_tax DECIMAL, "
    "l_returnflag STRING, "
    "l_linestatus STRING, "
    "l_shipdate DATE, "
    "l_commitdate DATE, "
    "l_receiptdate DATE, "
    "l_shipinstruct STRING, "
    "l_shipmode STRING, "
    "l_comment STRING"
)
csv_opts = {
    "delimiter": ",",
    "quote": '"',
    "escape": "\\",
    "mode": "FAILFAST",  # aborts the reading if any malformed data is found
    "dateFormat": "yyyy-MM-dd"
}

# prepare Hudi table for write
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} ("
          "  l_orderkey INT,"
          "  l_partkey INT,"
          "  l_suppkey INT,"
          "  l_linenumber INT,"
          "  l_quantity DECIMAL,"
          "  l_extendedprice DECIMAL,"
          "  l_discount decimal,"
          "  l_tax decimal,"
          "  l_returnflag VARCHAR(1),"
          "  l_linestatus VARCHAR(1),"
          "  l_shipdate DATE,"
          "  l_commitdate DATE,"
          "  l_receiptdate DATE,"
          "  l_shipinstruct VARCHAR(25),"
          "  l_shipmode VARCHAR(10),"
          "  l_comment STRING"
          ") USING hudi "
          "  TBLPROPERTIES ("
          "    type = 'MERGE_ON_READ',"
          "    primaryKey = 'l_orderkey,l_linenumber',"
          "    preCombineField = 'l_linenumber'"
          ") LOCATION '{hudi_table_path}'")

# we read batches of (batch_size_from_1_partition * kafka_partitions_num) size, and write them to Hudi
for end_offset in range(batch_size_from_1_partition,
                        batch_size_from_1_partition * num_of_batches + 1,
                        batch_size_from_1_partition):
    start_offset = end_offset - batch_size_from_1_partition
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", prepare_offset_config(kafka_topic, kafka_partitions_num, start_offset)) \
        .option("endingOffsets", prepare_offset_config(kafka_topic, kafka_partitions_num, end_offset) if end_offset < 5_500_000 else "latest") \
        .load() \
        .select(functions.col("value").cast("string").alias("csv_string")) \
        .select(functions.from_csv("csv_string", csv_schema, csv_opts).alias("parsed")) \
        .select("parsed.*")

    df.write \
        .format("org.apache.hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(hudi_table_path)
