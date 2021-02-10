import pymysql


def create_table():
    # 创建数据库
    create_db_sql = 'create database test_db default character set utf8;'
    try:
        cursor.execute(create_db_sql)
    except:
        pass
    # 建表
    sql = 'use test_db;'
    cursor.execute(sql)
    table1_sql = 'create table air ' \
                 '( locationId varchar(255) null,' \
                 'location   varchar(255) null,' \
                 'city       varchar(255) null,' \
                 'country    varchar(255) null,' \
                 'utc        varchar(255) null,' \
                 'local      varchar(255) null,' \
                 'parameter  varchar(255) null,' \
                 'value      varchar(255) null,' \
                 'unit       varchar(255) null,' \
                 'latitude   varchar(255) null,' \
                 'longitude  varchar(255) null);'
    try:
        cursor.execute(table1_sql)
    except:
        pass
    table2_sql = 'create table data3' \
                 '(id      int(4)      not null,' \
                 '`index` varchar(20) null,' \
                 'time    datetime    null,' \
                 'score   varchar(30) null);'
    try:
        cursor.execute(table2_sql)
    except:
        pass


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
