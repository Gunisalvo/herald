from flask import Flask
import configparser
import os

environment = os.environ.get("TEST_RIG_ENV", "local")
config = configparser.ConfigParser()
config.read("config.ini")
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = config["local"]["operationalDB"]


@app.route('/info')
def info():
    return "%s ..." % config["local"]["operationalDB"]


