import os
from pathlib import Path

import utils
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType


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
num_of_partitions = 8
batch_size_from_1_partition = 88000
num_of_batches = 10

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
    "hoodie.datasource.write.partitionpath.field": "l_quantity",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.recordkey.field": "l_orderkey,l_linenumber",
    "hoodie.datasource.write.precombine.field": "l_linenumber",
    "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider"
}

# data in Kafka topic is bytearray of csv string, this schema allows to parse this string
schema_of_csv_str = StructType([
    StructField("l_orderkey", IntegerType(), False),
    StructField("l_partkey", IntegerType(), False),
    StructField("l_suppkey", IntegerType(), False),
    StructField("l_linenumber", IntegerType(), False),
    StructField("l_quantity", DecimalType(), True),
    StructField("l_extendedprice", DecimalType(), True),
    StructField("l_discount", DecimalType(), True),
    StructField("l_tax", DecimalType(), True),
    StructField("l_returnflag", StringType(), True),
    StructField("l_linestatus", StringType(), True),
    StructField("l_shipdate", DateType(), True),
    StructField("l_commitdate", DateType(), True),
    StructField("l_receiptdate", DateType(), True),
    StructField("l_shipinstruct", StringType(), True),
    StructField("l_shipmode", StringType(), True),
    StructField("l_comment", StringType(), True)
])

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
           ") PARTITIONED BY (l_quantity)"
          f"  LOCATION '{hudi_table_path}'")

# we read batches of (batch_size_from_1_partition * num_of_partitions) size, and write them to Hudi
for end_offset in range(batch_size_from_1_partition,
                        batch_size_from_1_partition * num_of_batches + 1,
                        batch_size_from_1_partition):
    start_offset = end_offset - batch_size_from_1_partition
    df_csv = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", prepare_offset_config(kafka_topic, num_of_partitions, start_offset)) \
        .option("endingOffsets", prepare_offset_config(kafka_topic, num_of_partitions, end_offset)) \
        .load() \
        .select("value")

    df_curr = spark.read.csv(df_csv.rdd.map(lambda x: x.value.decode("utf-8")), schema=schema_of_csv_str)
    df_curr.write \
        .format("org.apache.hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(hudi_table_path)
