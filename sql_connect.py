'''
建立数据库连接
剩的每次都去打一个数据库连接语句
方便后期给别人使用时代码修改量
'''
import pymysql
from sqlalchemy import create_engine

# 数据库连接类


class Sql_c:
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
        connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
        self.cursor = connect.cursor()