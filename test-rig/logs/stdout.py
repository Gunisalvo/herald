import logging
import sys

logging.basicConfig(stream=sys.stdout)
logging.getLogger().setLevel(logging.INFO)


def logger():
    return logging.getLogger()
