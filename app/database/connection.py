# import mysql.connector
# from mysql.connector import Error
import pymysql
from pypika import Query, Table, Field
# from MySQLdb import _mysql

class Connection:

    conn = 0
    cursor = False
    connected = False
    autocommit = False

    def __init__(self, host="localhost", port=3306, user="root", password="", dbname="", autocommit=False):
        self.host = host
        self.port = port
        self.user = user
        self.dbname = dbname
        self.password = password
        self.autocommit = autocommit

    def connect(self):
        if self.conn:
            return self.conn

        try:
            # self.conn = mysql.connector.connect(host=self.host, port=self.port,database=self.dbname, user=self.user, password=self.password)
            # if self.conn.is_connected():
            #     db_Info = self.conn.get_server_info()
            #     print ("Connected to MySQL database... MySQL Server version on ",db_Info)
            #     self.cursor = self.conn.cursor()
            #     self.cursor.execute("select database();")
            #     record = self.cursor.fetchone()
            #     return self.conn
            
            # self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=self.dbname)
            self.conn = pymysql.connect(host='127.0.0.1', port=self.port, user='rcvn', passwd='T8gGJL3A', db=self.dbname)
            self.connected = True
            self.cursor = self.conn.cursor()
        except Exception as e:
            print ("Error while connecting to MySQL", e)

    def closed(self):
        if(self.connected):
            if self.cursor:
                self.cursor.close()
            self.conn.close()
            print ("MySQL connection is closed")

    def getCursor(self):
        if self.cursor:
            return self.cursor
        return self.conn.cursor()

    def commit(self):
        if self.autocommit:
            self.conn.commit()

    def execute(self, query):
        try:
            cursor = self.getCursor()
            # print (query.get_sql().replace('"', '`'))
            # cursor.execute(query.get_sql().replace('', '`'))
            cursor.execute(query.get_sql().replace('"', '`'))

            # self.commit()
            # cursor.execute("SELECT * FROM eg_product.category_translations WHERE category_id IN (2,3,4,5,6) AND locale ='th'")
            # cursor.execute("SHOW DATABASES;")
            # databases = cursor.fetchall()
            # print(databases)
            return cursor
        except Exception as e:
            print ("Error execute query here:", query.get_sql().replace('"', '`'))

    def fetchone(self, query):
        # print(query.replace('"', ''))
        try:
            cur = self.getCursor()
            # print(type(query))
            # query = query.replace('"', '`')
            print(query.replace('"', '`'))
            cur.execute(query.replace('"', '`'))
            # databases = cur.fetchall()
            rows = cur.fetchone()
            return rows
        except Exception as e:
            print ("Error fetch one", e)
        #     return None

    def fetchall(self, query):
        try:
            cur = self.execute(query)
            rows = cur.fetchall()
            # self.getCursor()
        except Exception as e:
            print ("Error fetch one", query.get_sql())
            return None

