from pathlib import Path

import dotenv


configs = dotenv.dotenv_values(str(Path(__file__).parent.parent.absolute() / ".env"))
