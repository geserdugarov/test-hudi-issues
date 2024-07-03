import os

import pyspark

from .configuration import configs


def init_spark_env(app_name: str) -> pyspark.sql.SparkSession:
    _set_spark_home()
    spark_session = (pyspark.sql.SparkSession.builder
                     .master(configs['SPARK_HOST_URL'])
                     .appName(app_name)
                     .config("spark.sql.warehouse.dir", configs['SPARK_WAREHOUSE_PATH'])
                     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                     .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")  # should be used by default, but it's broken sometimes
                     .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")  # to use Hudi specific syntax
                     # .config("spark.driver.extraJavaOptions", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006")  # for remote JVM debug
                     # .config("spark.worker.extraJavaOptions", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006")
                     .getOrCreate())
    spark_session.sparkContext.setLogLevel("INFO")
    return spark_session


def _set_spark_home() -> None:
    os.environ['SPARK_HOME'] = str(configs['SPARK_HOME'])
