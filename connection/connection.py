import pymysql
from pymysql.cursors import DictCursor

def connection():
    return pymysql.connect(
            host = '127.0.0.1',
            port = 3306,
            user ='root',
            database = 'conference',
            cursorclass= DictCursor
)