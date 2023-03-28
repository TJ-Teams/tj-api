
import os, random, string
from datetime import timedelta

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

class BaseConfig():

    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', None)
    if not JWT_SECRET_KEY:
        JWT_SECRET_KEY = ''.join(random.choice(string.ascii_lowercase) for i in range(32))

    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=1)

