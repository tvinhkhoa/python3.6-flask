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
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import mysql.connector
from mysql.connector import Error
import paramiko
import pymysql


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

def execute():
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

    _ssh_host = '128.199.210.78'
    _ssh_port = 22
    _ssh_username = 'rcvn'
    _ssh_password = 'CJVEbBkG'
    _remote_bind_address = '128.199.210.78'
    _remote_mysql_port = 3306
    _local_bind_address = '127.0.0.1'
    _local_mysql_port = 3306
    _db_user = 'rcvn'
    _db_password = 'T8gGJL3A'
    # _db_name = 'eg_product'
    _db_name = ''
    
    # sshtunnel.SSH_TIMEOUT = 5.0
    # sshtunnel.TUNNEL_TIMEOUT = 5.0
    # server = SSHTunnelForwarder(('128.199.210.78', 22), ssh_password='CJVEbBkG', ssh_username='rcvn', remote_bind_address=('127.0.0.1', 3306))
    server = SSHTunnelForwarder(
        (_ssh_host, _ssh_port),
        ssh_username=_ssh_username,
        ssh_password=_ssh_password,
        remote_bind_address=(_local_bind_address, _remote_mysql_port)
    )

    try:
        server.start()
        print(server.local_bind_port)
        conn = pymysql.connect(host=_local_bind_address, user=_db_user,
            passwd=_db_password, db=_db_name,
            port=server.local_bind_port)
        
        cur = conn.cursor()
        cur.execute("SELECT VERSION()")
        # query = '''SELECT VERSION();'''
        # data = pd.read_sql_query(query, conn)
        version = cur.fetchone()
    
        print("Database version: {}".format(version[0]))

        conn.close()
        server.stop()
    except Error as e:
        print("Error connecting SSH", e)

    # try:
    #     with sshtunnel.open_tunnel (
    #             debug_level='DEBUG',
    #             ssh_address_or_host=(_host, _ssh_port),
    #             ssh_username=_username,
    #             ssh_password=_password,
    #             remote_bind_address=(_local_bind_address, _remote_mysql_port)
    #             # local_bind_address=(_local_bind_address, _local_mysql_port)
    #             # local_bind_address=('0.0.0.0', 3307)
    #     ) as tunnel:
    #         try:
    #             conn = mysql.connector.connect(
    #                 user=_db_user,
    #                 password=_db_password,
    #                 host=_local_bind_address,
    #                 database='',
    #                 port=tunnel.local_bind_port)

    #             if conn.is_connected():
    #                 db_Info = conn.get_server_info()
    #                 print ("Connected to MySQL database... MySQL Server version on ", db_Info)

    #             conn.close()
    #         # except Error as e1:
    #         #     print ("Error connecting to Mysql", e1)
    #             print("Connect SSH success")
                
    #         except Error as e1:
    #             print ("Error connecting to Mysql", e1)
    # except Error as e:
    #     print ("Error connecting to SSH", e)


if __name__ == '__main__':
    app = create_app()

    if "cli.py" in sys.argv:
        execute()
        stop_app()
    # 