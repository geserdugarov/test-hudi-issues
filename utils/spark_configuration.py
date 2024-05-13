import os
from pathlib import Path

import dotenv


def set_spark_home() -> None:
    curr_dir = Path(__file__).parent.parent.absolute()
    spark_home = dotenv.dotenv_values(str(curr_dir / ".env"))['SPARK_HOME']
    os.environ['SPARK_HOME'] = str(spark_home)
