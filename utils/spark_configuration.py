import os

import pyspark

from .configuration import configs


def _builder_spark_env_basic(app_name: str) -> pyspark.sql.SparkSession.Builder:
    _set_spark_home()
    builder = (pyspark.sql.SparkSession.builder
               .master(configs['SPARK_HOST_URL'])
               .appName(app_name)
               # .enableHiveSupport()  ## force Spark to use spark-hive_2.12-*.jar
               .config("spark.driver.cores", "1")
               .config("spark.driver.memory", "2g")
               .config("spark.driver.memoryOverhead", "512m")
               .config("spark.driver.host", "<DRIVER-IP>")
               .config("spark.driver.bindAddress", "0.0.0.0")
               .config("spark.executor.instances", "8")
               .config("spark.executor.cores", "3")
               .config("spark.executor.memory", "8g")
               .config("spark.executor.memoryOverhead", "1g")
               .config("spark.eventLog.enabled", "true")
               # event logs dir should be shared between driver and executors
               .config("spark.eventLog.dir", "hdfs://hdfs-namenode-ip:9000/<user>/spark-data/history")
               # default location for managed tables, crucial to set if Hive Metastore is used
               .config("spark.sql.warehouse.dir", "hdfs://hdfs-namenode-ip:9000/<user>/spark-data/warehouse")
               # for remote JVM debug
               # .config("spark.driver.extraJavaOptions", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006")
               # .config("spark.executor.extraJavaOptions", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007")
               )
    return builder


def init_spark_env_for_hudi(app_name: str) -> pyspark.sql.SparkSession:
    builder = _builder_spark_env_basic(app_name)
    builder.config("spark.jars",
                   # support Hudi
                   "/home/<user>/git/hudi-open-source/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.2.0-SNAPSHOT.jar,"
                   # support Kafka
                   "/home/<user>/soft/spark-3.5.7-extra-jars/spark-sql-kafka-0-10_2.12-3.5.7.jar,"
                   "/home/<user>/soft/spark-3.5.7-extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar,"
                   "/home/<user>/soft/spark-3.5.7-extra-jars/kafka-clients-3.4.1.jar,"
                   "/home/<user>/soft/spark-3.5.7-extra-jars/commons-pool2-2.11.1.jar,"
                   # Hive sync for Hudi
                   "/home/<user>/soft/spark-3.5.7-extra-jars/hive-exec-2.3.4-core.jar,"
                   "/home/<user>/soft/spark-3.5.7-extra-jars/hive-serde-2.3.4.jar"
                   )
    # serialization/deserialization configuration
    builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # to use Hudi specific syntax
    builder.config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    # try this if got NullPointerException in Kryo SerDe, but HoodieSparkKryoRegistrar should be used by default
    # builder.config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    spark_session = builder.getOrCreate()
    spark_session.sparkContext.setLogLevel("INFO")
    return spark_session


def init_spark_env_for_lance(app_name: str) -> pyspark.sql.SparkSession:
    builder = _builder_spark_env_basic(app_name)
    builder.config("spark.jars",
                   # support Lance
                   "/home/<user>/soft/spark-3.5.7-extra-jars/lance-spark-bundle-3.5_2.12-0.0.15.jar,"
                   "/home/<user>/soft/spark-3.5.7-extra-jars/lance-namespace-hive3-0.0.20.jar,"
                   # Hive 3.* sync for Lance
                   "/home/<user>/soft/spark-3.5.7-extra-jars/hive-exec-3.1.3.jar,"
                   "/home/<user>/soft/spark-3.5.7-extra-jars/hive-standalone-metastore-3.1.3.jar"
                   )
    # Hive Metastore configuration
    builder.config("hive.metastore.uris", "thrift://localhost:9083")
    # Lance catalog support and storing configs
    builder.config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
    builder.config("spark.sql.catalog.lance.impl", "hive3")
    builder.config("spark.sql.catalog.lance.hadoop.hive.metastore.uris", "thrift://localhost:9083")
    builder.config("spark.sql.catalog.lance.root", "/home/<user>/git/test-hudi-issues/tmp/lance_hive_demo")
    # Hive 3.x namespace specific configs
    builder.config("spark.sql.catalog.lance.parent", "hive")
    builder.config("spark.sql.catalog.lance.parent_delimiter", ".")
    builder.config("spark.sql.defaultCatalog", "lance")
    spark_session = builder.getOrCreate()
    spark_session.sparkContext.setLogLevel("INFO")
    return spark_session


def _set_spark_home() -> None:
    os.environ['SPARK_HOME'] = str(configs['SPARK_HOME'])
    os.environ['JAVA_HOME'] = str(configs['JAVA_HOME'])
