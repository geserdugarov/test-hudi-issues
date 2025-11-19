import os
from pathlib import Path

import utils


# prepare environment
script_name = os.path.basename(__file__)[:-3]
spark = utils.init_spark_env_for_hudi(script_name)

table_path = str(Path("/tmp/schema_compatibility"))
df_load = spark.read.format("org.apache.hudi").load(table_path)
for row in df_load.collect():
    print(row)

# need more description, the issue couldn't be reproduced in current 0.15
