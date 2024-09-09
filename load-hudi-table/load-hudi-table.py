import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
spark = utils.init_spark_env(script_name)

# load Hudi table
table_path = str(Path("/home/xxx/yyy/table_name"))
df_load = spark.read.format("org.apache.hudi").load(table_path)
df = df_load.select([col for col in df_load.columns if col in ['_hoodie_record_key', 'col1', 'col2', 'col3', 'par']]).orderBy("_hoodie_record_key").collect()
for row in df:
    print(row)
