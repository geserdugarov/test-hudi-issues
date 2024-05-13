# Hudi issues check
Repository for Python scripts to check [Hudi issues](https://github.com/apache/hudi/issues) reproducibility.

## Setup
PyCharm is used for running scrtipts. Spark is runned locally with one thread.
1. [Download](https://spark.apache.org/downloads.html) corresponding Apache Spark version with Hadoop and extract to some folder, `SPARK_HOME`.
2. Prepare Python virtual environment: 

   - `python3 -m venv [env_name]`
   - `source [env_name]/bin/activate`
   - `pip install -r requirements.txt`

3. Set `SPARK_HOME` in `.env` file.
4. Add `pyspark.zip` and `py4j-*-src.zip` from `{SPARK_HOME}/python/lib/` into `Project` >> `Project Structure` in PyCharm.
5. Build [Hudi project](https://github.com/apache/hudi/) and copy Spark bundle to `{SPARK_HOME}/jars/`.
6. Start Spark cluster locally, `{SPARK_HOME}/sbin/start-all.sh`.

## Checking
Run corresponding script from configured PyCharm.
