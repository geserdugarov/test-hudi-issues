# Hudi issues check
Repository for Python scripts to check [Hudi issues](https://github.com/apache/hudi/issues) reproducibility.

## Setup
1. [Download](https://archive.apache.org/dist/spark/) corresponding Apache Spark version with Hadoop, 
deploy it in the corresponding destination. Spark could be run locally or remote.
2. Prepare Python virtual environment: 

   ```bash
   python3 -m venv [env_name]
   source [env_name]/bin/activate
   pip install -r requirements.txt
   ```

3. Set `SPARK_HOME`, `SPARK_HOST_URL` and `SPARK_WAREHOUSE_PATH` in the `.env` file, and also `JAVA_HOME`. 
Examples are presented in `.env-example`.
4. JDK could be switched by changing `JAVA_HOME` in `.env` file.
5. For PyCharm, add `pyspark.zip` and `py4j-*-src.zip` from `{SPARK_HOME}/python/lib/` 
into `Project` >> `Project Structure` >> `Content root`.
Also, you could run scrips using bash by:

   ```bash
   cd ~/git/test-hudi-issues/
   export PYTHONPATH="$(pwd)"
   export SPARK_HOME="$HOME/soft/spark-3.5.7-bin-hadoop3-os"
   export PYT4J_VERSION="0.10.9.7"
   export PYTHONPATH="$PYTHONPATH:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-$PYT4J_VERSION-src.zip"
   python3 -m common.read-from-kafka-write-to-hudi
   ```

6. Build intended version of [Hudi project](https://github.com/apache/hudi/), 
and make sure that `hudi-spark*-bundle_*-*.jar` is passed in `spark.jars` during Spark session creation.

## Checking
Run script, which corresponds to an issue, from configured PyCharm.

To switch versions:
1. Change `SPARK_HOME` in the `.env` file.
2. For PyCharm, add related `pyspark.zip` and `py4j-*-src.zip` to `Content root`.
3. Build new version of [Hudi project](https://github.com/apache/hudi/),
and copy new `hudi-spark*.*-bundle_2.1*-*.jar` to corresponding Spark home directory.
4. Stop previously started Spark cluster, and start proper version of Spark. 

## Read from Kafka
To work with Kafka the corresponding JARs should be downloaded, and passed in `spark.jars` during Spark session creation:

- [spark-streaming-kafka-0-10_2.12-*.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/)
- [spark-token-provider-kafka-0-10_2.12-*.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/)
- [kafka-clients-*.jar](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/)
- [commons-pool2-*.jar](https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/)
