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
import xlsxwriter
from app.libs.SSHTunnel import SSHTunnel

application = Application()
MYSQL_HOST = os.getenv('DB_MYSQL_HOST', 'localhost')
MYSQL_POST = os.getenv('DB_MYSQL_PORT', 3306)
MYSQL_USER = os.getenv('DB_MYSQL_USER', 'admin')
MYSQL_PASS = os.getenv('DB_MYSQL_PASSWORD', '')

def create_app():
    """Construct the core application."""
    app = application.getApp(os.getenv('APP_ENV'))
    # Create tables for our models
    # with app.app_context():
    #     application.db = Connection(MYSQL_HOST, MYSQL_POST, MYSQL_USER, MYSQL_PASS)
    #     application.db.connect()
    return app

def stop_app():
    global application
    # with app.app_context():
    #     if application.db:
    #         application.db.closed()
    return

def execute(app):
    # cate_trans = CategoryTranslate(application.db)
    # rows = cate_trans.getCategories()
    # workbook = xlsxwriter.Workbook('categories.freeze.xlsx')
    # worksheet = workbook.add_worksheet()
    # worksheet.write('A1', 'Categories')
    # # worksheet.freeze_panes(1, 0)
    # worksheet.freeze_panes(1, 1)

    # i = 3
    # for row in rows:
    #     worksheet.write('B'+str(i), row[3])
    #     i += 1
    # workbook.close()

    # Begin JPM
    tunnelJPM = SSHTunnel()
    tunnelJPM.forwarder(
        {"host":os.getenv('JPM_SSH_HOST'),"port":os.getenv('JPM_SSH_PORT')},
        ssh_username=os.getenv('JPM_SSH_USER'),
        ssh_password=os.getenv('JPM_SSH_PASSWORD'),
        remote={"bind_address":os.getenv('LOCAL'),"bind_port":os.getenv('JPM_MYSQL_PORT')}
    )

    try:
        tunnelJPM.start()
        with app.app_context():
            application.db = Connection(
                os.getenv('LOCAL'),
                tunnelJPM.get_local_bind_port(),
                os.getenv('JPM_MYSQL_USER'),
                os.getenv('JPM_MYSQL_PASSWORD'),
                dbname='eg_product_shadow'
            )
            application.db.connect()
        cate_trans = CategoryTranslate(application.db)
        rows = cate_trans.getCategories()
        application.db.close()
    except Exception as e:
        print("Error connecting SSH", e)
    finally:
        tunnelJPM.stop()
    # End JPM

if __name__ == '__main__':
    app = create_app()

    if "cli.py" in sys.argv:
        execute(app)
        stop_app()
    # 