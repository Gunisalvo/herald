from flask import Flask
import configparser
import os

environment = os.environ.get("TEST_RIG_ENV", "local")
config = configparser.ConfigParser()
config.read("config.ini")
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = config[environment]["operationalDB"]


@app.route('/info')
def info():
    return "%s ..." % config[environment]["operationalDB"]


if __name__ == '__main__':
    app.run(debug=True, port=config[environment].getint("socialNetworkPort"), host="0.0.0.0")
