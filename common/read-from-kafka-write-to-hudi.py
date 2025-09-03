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
spark = utils.init_spark_env(script_name)

# Kafka read configuration
kafka_brokers = "ip1:port1,ip2:port2"
kafka_topic = "tpch_60kk"
kafka_partitions_num = 8
batch_size_from_1_partition = 100000
num_of_batches = 3

# Hudi configuration
# If HDFS is used, then `fs.default.name` should be set in Spark's ./conf/core-site.xml, as:
#   <property>
#       <name>fs.default.name</name>
#       <value>hdfs://hdfs-namenode-ip:9000</value>
#   </property>
# For local write, use file:///tmp/some-path
hudi_table_path = "hdfs://hdfs-namenode-ip:9000/some_path"
table_name = "kafka_to_hudi"
hudi_options = {
    "hoodie.table.name": f"{table_name}",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.recordkey.field": "l_orderkey,l_linenumber",
    "hoodie.datasource.write.precombine.field": "l_linenumber",
    "hoodie.index.type": "BUCKET",
    "hoodie.index.bucket.engine": "SIMPLE",
    "hoodie.bucket.index.num.buckets": f"{kafka_partitions_num}",
    "hoodie.bucket.index.hash.field": "l_orderkey,l_linenumber",
    "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider"
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
        .option("endingOffsets", prepare_offset_config(kafka_topic, kafka_partitions_num, end_offset)) \
        .load() \
        .select(functions.col("value").cast("string").alias("csv_string")) \
        .select(functions.from_csv("csv_string", csv_schema, csv_opts).alias("parsed")) \
        .select("parsed.*") \
        .repartition(kafka_partitions_num * 2, "l_orderkey", "l_linenumber")

    df.write \
        .format("org.apache.hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(hudi_table_path)
