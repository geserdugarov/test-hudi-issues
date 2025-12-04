import utils
from pyspark.sql import functions


def prepare_offset_config(kafka_topic: str, partitions_num: int, set_offset: int) -> str:
    config = f"""{{"{kafka_topic}": {{"""
    for partition_id in range(partitions_num-1):
        config += f""" "{partition_id}":{set_offset}, """
    config += f""" "{partitions_num-1}":{set_offset}}}}} """
    return config


table_name = "kafka_to_hudi_mor_upsert"

# prepare environment
spark = utils.init_spark_env_for_hudi(table_name)

# Kafka read configuration
kafka_brokers = "ip1:port1,ip2:port2"
kafka_topic = "tpch_60kk"
kafka_partitions_num = 8
# there are different max_offset in each topic partition
# so, we set a threshold here to switch the last batch read
# from the current start until latest in partitions
min_of_end_offsets = 7_100_000
# we read recs_per_partit_first records per partition first
# and then recs_per_partit_ups records per partition
# in all following (total_commits_num - 1) commits
recs_per_partit_first = 4_500_000
recs_per_partit_commit = 1_000_000
total_commits_num = 4

# Hudi write configuration
table_type = "MERGE_ON_READ"
operation_type = "upsert"
# If HDFS is used, then `fs.default.name` should be set in Spark's ./conf/core-site.xml, as:
#   <property>
#       <name>fs.default.name</name>
#       <value>hdfs://hdfs-namenode-ip:9000</value>
#   </property>
# For local write, use file:///tmp/some-path
hudi_table_path = f"hdfs://hdfs-namenode-ip:9000/<user>/hudi/{table_name}"
hudi_options = {
    "hoodie.table.name": f"{table_name}",
    "hoodie.datasource.write.table.type": f"{table_type}",
    # operation
    "hoodie.datasource.write.operation": f"{operation_type}",
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
          f"   type = '{table_type}',"
          "    primaryKey = 'l_orderkey,l_linenumber',"
          "    preCombineField = 'l_linenumber'"
          ") LOCATION '{hudi_table_path}'")

# read Kafka topic partitions in batches by configured offsets,
# and write to Hudi directly
end_offset = 0
for commit_id in range(total_commits_num):
    if commit_id == 0:
        # first the biggest commit
        start_offset = 0
        end_offset = recs_per_partit_first
    else:
        # following commits
        start_offset = end_offset
        end_offset += recs_per_partit_commit
    df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", kafka_brokers)
          .option("subscribe", kafka_topic)
          .option("startingOffsets",
                  prepare_offset_config(kafka_topic, kafka_partitions_num, start_offset))
          .option("endingOffsets",
                  prepare_offset_config(kafka_topic, kafka_partitions_num, end_offset)
                  if end_offset < min_of_end_offsets
                  else "latest")
          .load()
          .select(functions.col("value").cast("string").alias("csv_string"))
          .select(functions.from_csv("csv_string", csv_schema, csv_opts).alias("parsed"))
          .select("parsed.*"))

    (df.write.format("hudi")
     .options(**hudi_options)
     .mode("append")
     .save(hudi_table_path))
