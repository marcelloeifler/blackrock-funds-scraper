import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))

ENV_CST = os.environ.get("ENV_CST")
