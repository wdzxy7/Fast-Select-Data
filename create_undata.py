'''
实验初期自己造不同分布数据进行测试
'''
import hashlib
import pylab as pl
import pymysql
import numpy as np
import random


# 生成学生ID
def creat_student_id():
    id_list = set()
    while len(id_list) < 2500000:
        number = random.randint(1, 1999999999)
        if number in id_list:
            continue
        else:
            number = hashlib.md5(str(number).encode('utf-8'))
            id_list.add(number.hexdigest())
    return id_list


# 正态分布数据
def normal_data():
    data = np.random.normal(500, 100, 30000)
    return data


# 随机分布数据
def random_data():
    data = np.random.randint(0, 1000, [30000000])
    return data


# 指数分布数据
def exponential_data():
    data = np.random.exponential(400, size=30000000)
    return data


# 泊松分布数据
def poisson_data():
    data = np.random.poisson(lam=500, size=30000000)
    return data


def store_data(split_data):
    year = 2017
    term = 1
    store = []
    all_count = 0
    for small in split_data:
        print(all_count)
        if term == 4:
            year = year + 1
            term = 1
        for stu, grade in zip(students, small):
            tup = (stu, major_list[year - 2017][term - 1], term, float(grade), year)
            store.append(tup)
        term = term + 1
        all_count = all_count + 1
    sql = 'INSERT INTO grades.poisson_data(student, major, term, score, year) VALUES (%s, %s, %s, %s, %s);'
    cursor.executemany(sql, store)


def main():
    sql = 'TRUNCATE TABLE grades.poisson_data;'
    # cursor.execute(sql)
    # data = normal_data()
    # data = random_data()
    # data = poisson_data()
    data = exponential_data()
    print(data)
    # 数据拆分12份
    splited = np.split(data, 12, axis=0)
    # store_data(splited)
    pl.hist(data, bins=800)
    pl.title('exponential')
    pl.show()
    '''
    print(min(data))
    print(max(data))
    '''


if __name__ == '__main__':
    major_list = [['math', 'chinese', 'english'], ['biological', 'geography', 'art']
        , ['music', 'history', 'physics'], ['political', 'chemistry', 'Line_generation']]
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    students = creat_student_id()
    main()