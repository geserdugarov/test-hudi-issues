import os
import shutil
from pathlib import Path


def prepare_temp_dirs(tmp_dir_path: str) -> None:
    if os.path.exists(Path(tmp_dir_path)):
        shutil.rmtree(Path(tmp_dir_path))

    warehouse_path = Path("/tmp/spark-warehouse")
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)