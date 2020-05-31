from sqlalchemy import create_engine
import configparser
import logging

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARN)


def get_connection(env):
    config = configparser.ConfigParser()
    config.read("config.ini")

    engine = create_engine(config[env]["operationalDB"])
    return engine.connect()