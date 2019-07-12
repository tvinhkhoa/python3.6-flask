import os
from flask import Flask
from instance.config import app_config

class Application(object):

    app = False
    def __init__(self):
        pass

    def getApp(self, config_name=''):
        if self.app==False:
            self.app = Flask(__name__, instance_relative_config=True)
            self.app.config.from_object(app_config[config_name])
            self.app.app_config = app_config
        return self.app
