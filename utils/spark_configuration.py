import os

import pyspark
import utils

from .configuration import configs


def init_spark_env(app_name: str) -> pyspark.sql.SparkSession:
    _set_spark_home()
    # for History Server
    utils.create_dir_if_absent("/tmp/spark-events")
    spark_session = (pyspark.sql.SparkSession.builder
                     .master(configs['SPARK_HOST_URL'])
                     .appName(app_name)
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
                     .config("spark.eventLog.dir", "file:/tmp/spark-history")
                     # serialization/deserialization configurations
                     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                     # Hudi
                     .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")  # to use Hudi specific syntax
                     # .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")  # should be used by default, but it's broken sometimes
                     # .config("spark.driver.extraJavaOptions", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006")  # for remote JVM debug
                     # .config("spark.executor.extraJavaOptions", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007")
                     .config("spark.jars",
                             # support Hudi
                             "/home/<user>/git/hudi-open-source/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.2.0-SNAPSHOT.jar,"
                             # support Kafka 
                             "/home/<user>/soft/spark-3.5.7-extra-jars/spark-sql-kafka-0-10_2.12-3.5.7.jar,"
                             "/home/<user>/soft/spark-3.5.7-extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar,"
                             "/home/<user>/soft/spark-3.5.7-extra-jars/kafka-clients-3.4.1.jar,"
                             "/home/<user>/soft/spark-3.5.7-extra-jars/commons-pool2-2.11.1.jar,"
                             # Hive sync transitive dependencies in Hudi
                             "/home/<user>/soft/spark-3.5.7-extra-jars/hive-exec-2.3.4-core.jar,"
                             "/home/<user>/soft/spark-3.5.7-extra-jars/hive-serde-2.3.4.jar"
                             )
                     .getOrCreate())
    spark_session.sparkContext.setLogLevel("INFO")
    return spark_session


def _set_spark_home() -> None:
    os.environ['SPARK_HOME'] = str(configs['SPARK_HOME'])
    os.environ['JAVA_HOME'] = str(configs['JAVA_HOME'])
