import pymysql


def create_table():
    # 创建数据库
    create_db_sql = 'create database unknown_data default character set utf8;'
    try:
        cursor.execute(create_db_sql)
    except:
        pass
    # 建表
    sql = 'use unknown_data;'
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
    table3_sql = 'create table dbscan_result(score varchar(30) null);'
    try:
        cursor.execute(table3_sql)
    except:
        pass
    table4_sql = 'create table avg_dbscan_result(score varchar(30) null);'
    try:
        cursor.execute(table4_sql)
    except:
        pass
    table5_sql = 'create table optics_result(score varchar(30) null);'
    try:
        cursor.execute(table5_sql)
    except:
        pass
    table6_sql = 'create table avg_optics_result(score varchar(30) null);'
    try:
        cursor.execute(table6_sql)
    except:
        pass
    table7_sql = 'create table random_result(score varchar(30) null);'
    try:
        cursor.execute(table7_sql)
    except:
        pass
    table8_sql = 'create table avg_result(score varchar(30) null);'
    try:
        cursor.execute(table8_sql)
    except:
        pass
    table9_sql = 'create table k_means_result(score varchar(30) null);'
    try:
        cursor.execute(table9_sql)
    except:
        pass
    table10_sql = 'create table avg_k_means_result(score varchar(30) null);'
    try:
        cursor.execute(table10_sql)
    except:
        pass


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    create_table()