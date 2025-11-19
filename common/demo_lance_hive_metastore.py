import os

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
spark = utils.init_spark_env_for_lance(script_name)

# create namespace only,
# catalog is set in Spark session configuration by `spark.sql.defaultCatalog`
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo;")
spark.sql("USE demo;")

# Lance doesn't support creation of empty table,
# so we create non-empty table using CTAS
spark.sql("CREATE TABLE IF NOT EXISTS demo.adventurers "
          "  USING lance "
          "  TBLPROPERTIES ("
          "    'emb.arrow.fixed-size-list.size'='384')"
          "  AS"
          "  SELECT"
          "    CAST(1 AS BIGINT) AS id,"
          "    CAST('knight' AS STRING) AS text,"
          "    CAST(array(0.9, 0.4, 0.8) AS ARRAY<FLOAT>) AS vector;")

spark.sql("INSERT INTO demo.adventurers VALUES "
          "  (2, 'ranger', array(0.8, 0.4, 0.7)),"
          "  (9, 'priest', array(0.6, 0.2, 0.6)),"
          "  (4, 'rogue',  array(0.7, 0.4, 0.7));")

spark.sql("SHOW DATABASES;").show(truncate=False)
spark.sql("SHOW TABLES in demo;").show(truncate=False)
spark.sql("DESCRIBE EXTENDED demo.adventurers;").show(truncate=False)
spark.sql("SELECT * FROM demo.adventurers LIMIT 10;").show(truncate=False)
