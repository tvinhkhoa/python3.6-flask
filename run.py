#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import errno
import os
from flask import Flask, current_app, request, escape
import app.database.connection
from app.database.connection import Connection
from app.models.Categories import CategoryTranslate
from app.Application import Application

application = Application()
MYSQL_HOST = os.getenv('DB_MYSQL_HOST', 'localhost')
MYSQL_POST = os.getenv('DB_MYSQL_PORT', 3306)
MYSQL_USER = os.getenv('DB_MYSQL_USER', 'admin')
MYSQL_PASS = os.getenv('DB_MYSQL_PASSWORD', '')
WEB_PORT = os.getenv('WEB_PORT', 3000)

def create_app():
    """Construct the core application."""
    app = application.getApp(os.getenv('APP_ENV'))

    # Imports
    # from . import routes
    
    # Create tables for our models
    with app.app_context():
        application.db = Connection(MYSQL_HOST, MYSQL_POST, MYSQL_USER, MYSQL_PASS)
        application.db.connect()

    return app

def stop_app():
    global application
    with app.app_context():
        if application.db:
            application.db.closed()
    return

if __name__ == '__main__':
    app = create_app()

    @app.route('/', methods=['GET'])
    def hello():
        print("Go route")
        name = request.args.get("name", "World")
        return 'Hello, Flask'

    app.run(host=os.getenv('WEB_HOST'), port=application.app.app_config[os.getenv('APP_ENV')].WEB_POST, debug=os.getenv('WEB_DEBUG'), threaded=True)
    @app.after_request
    def after_request(response):
        stop_app()
        return response