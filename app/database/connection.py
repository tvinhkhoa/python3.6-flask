import mysql.connector
from mysql.connector import Error
# import pymysql
from pypika import Query, Table, Field

class Connection:

    conn = 0
    cursor = 0

    def __init__(self, host="localhost", port=3306, user="root", password="", dbname=""):
        self.host = host
        self.port = port
        self.user = user
        self.dbname = dbname
        self.password = password

    def connect(self):
        if self.conn:
            return self.conn

        try:
            self.conn = mysql.connector.connect(host=self.host, database=self.dbname, user=self.user, password=self.password)
            if self.conn.is_connected():
                db_Info = self.conn.get_server_info()
                print ("Connected to MySQL database... MySQL Server version on ",db_Info)
                self.cursor = self.conn.cursor()
                self.cursor.execute("select database();")
                record = self.cursor.fetchone()
                return self.conn
        except Error as e :
            print ("Error while connecting to MySQL", e)

    def closed(self):
        if(self.conn.is_connected()):
            if self.cursor:
                self.cursor.close()
            self.conn.close()
            print ("MySQL connection is closed")

    def getCursor(self):
        if self.cursor:
            return self.cursor
        return self.conn.cursor()

    def execute(self, query):
        cursor = self.getCursor()
        cursor.execute(query.get_sql().replace('"', '`'))
        try:
            return cursor.fetchall()
        except Error as e:
            print ("Error execute query", query.get_sql())

    # def fetchone(self):
