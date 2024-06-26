import os
import shutil
from pathlib import Path

from .configuration import configs


def prepare_temp_dirs(tmp_dir_path: str, create: bool = False) -> None:
    if os.path.exists(Path(tmp_dir_path)):
        shutil.rmtree(Path(tmp_dir_path))
    if create:
        os.mkdir(Path(tmp_dir_path))

    warehouse_path = Path(configs['SPARK_WAREHOUSE_PATH'])
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)
