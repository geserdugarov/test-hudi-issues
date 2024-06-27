# Hudi issues check
Repository for Python scripts to check [Hudi issues](https://github.com/apache/hudi/issues) reproducibility.

## Setup
PyCharm is used for running scrtipts. Spark is run locally with one thread.
1. [Download](https://spark.apache.org/downloads.html) corresponding Apache Spark version with Hadoop and extract to some folder, `SPARK_HOME`.
2. Prepare Python virtual environment: 

   - `python3 -m venv [env_name]`
   - `source [env_name]/bin/activate`
   - `pip install -r requirements.txt`

3. Set `SPARK_HOME`, `SPARK_HOST_URL` and `SPARK_WAREHOUSE_PATH` in the `.env` file. Examples are presented in `.env-example`.
4. For PyCharm, add `pyspark.zip` and `py4j-*-src.zip` from `{SPARK_HOME}/python/lib/` into `Project` >> `Project Structure` >> `Content root`.
5. Build intended version of [Hudi project](https://github.com/apache/hudi/) and copy `hudi-spark*.*-bundle_2.1*-*.jar` to `{SPARK_HOME}/jars/`.
6. Start Spark cluster locally, `{SPARK_HOME}/sbin/start-all.sh`.

## Checking
Run script, which corresponds to an issue, from configured PyCharm.

To switch versions:
1. Change `SPARK_HOME` in the `.env` file.
2. For PyCharm, add related `pyspark.zip` and `py4j-*-src.zip` to `Content root`.
3. Build new version of [Hudi project](https://github.com/apache/hudi/) and copy new `hudi-spark*.*-bundle_2.1*-*.jar` to corresponding Spark home directory.
4. Stop previously started Spark cluster, and start proper version of Spark. 
