import pymysql
import sqlalchemy
pymysql.install_as_MySQLdb()

class mysql_engine:
    def __init__(self,url):
        self.url = url
    def create_engine(self):
        mysql_engine = sqlalchemy.create_engine(self.url)
        return mysql_engine