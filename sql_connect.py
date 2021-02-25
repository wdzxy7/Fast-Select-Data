import pymysql
from sqlalchemy import create_engine


class Sql_c:
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
        connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
        self.cursor = connect.cursor()