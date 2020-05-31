from trumania.core import circus
import configparser
import pandas as pd


class Synthesizer(object):

    def __init__(self, env):
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")
        self.dataset = circus.Circus(
            name=self.config[env]["appName"],
            master_seed=self.config[env].getint("randomSeed"),
            start=pd.Timestamp(self.config[env]["generatorTimestamp"]),
            step_duration=pd.Timedelta(self.config[env]["generatorDelta"]))
