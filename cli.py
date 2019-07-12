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

application = Application()
MYSQL_HOST = os.getenv('DB_MYSQL_HOST', 'localhost')
MYSQL_POST = os.getenv('DB_MYSQL_PORT', 3306)
MYSQL_USER = os.getenv('DB_MYSQL_USER', 'admin')
MYSQL_PASS = os.getenv('DB_MYSQL_PASSWORD', '')

def create_app():
    """Construct the core application."""
    app = application.getApp(os.getenv('APP_ENV'))
    
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

def execute():
    cate_trans = CategoryTranslate(application.db)
    rows = cate_trans.getCategories()
    workbook = xlsxwriter.Workbook('categories.xlsx')
    worksheet = workbook.add_worksheet()
    worksheet.write('A1', 'Categories')
    i = 2
    for row in rows:
        worksheet.write('A'+str(i), row[3])
        i += 1
    workbook.close()

if __name__ == '__main__':
    app = create_app()

    if "cli.py" in sys.argv:
        execute()
        stop_app()
    # 