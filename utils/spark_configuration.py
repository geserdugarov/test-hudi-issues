import os

import pyspark

from .configuration import configs


def init_spark_env(app_name: str) -> pyspark.sql.SparkSession:
    _set_spark_home()
    return (pyspark.sql.SparkSession.builder
            .master(configs['SPARK_HOST_URL'])
            .appName(app_name)
            .config("spark.sql.warehouse.dir", configs['SPARK_WAREHOUSE_PATH'])
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")  # to use Hudi specific syntax
            .getOrCreate())


def _set_spark_home() -> None:
    os.environ['SPARK_HOME'] = str(configs['SPARK_HOME'])
