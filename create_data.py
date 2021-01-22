import pymysql
import random
import hashlib
major_list = [['math', 'chinese', 'english'], ['biological', 'geography', 'art']
              , ['music', 'history', 'physics'], ['political', 'chemistry', 'Line_generation']]


# 生成学生ID
def creat_balanced_student_id():
    id_list = set()
    while len(id_list) < 2500000:
        number = random.randint(1, 1999999999)
        if number in id_list:
            continue
        else:
            number = hashlib.md5(str(number).encode('utf-8'))
            id_list.add(number.hexdigest())
    return id_list


# 生成四个学期均衡数据
def creat_score(student_id, year, major):
    store_list = []
    for stu in student_id:
        score = random.randint(0, 1000)
        tup = (stu, major[0], 1, score, year)
        store_list.append(tup)
        score = random.randint(0, 1000)
        tup = (stu, major[1], 2, score, year)
        store_list.append(tup)
        score = random.randint(0, 1000)
        tup = (stu, major[2], 3, score, year)
        store_list.append(tup)
    return store_list


def creat_unbalanced_student_id():
    id_list = set()
    first = random.randint(1, 5000000)
    upper_limit = (1000000 - first) / 2
    second = random.randint(1, int(upper_limit))
    third = 10000000 - first - second
    while len(id_list) < first:
        number = random.randint(1, 999999999)
        if number in id_list:
            continue
        else:
            number = hashlib.md5(str(number).encode('utf-8'))
            id_list.add(number.hexdigest())
    return id_list


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE grades.transcript_balanced;'
    cursor.execute(sql)
    for sample_num in range(1, 21):
        sql = 'TRUNCATE TABLE grades.sample' + str(sample_num) + ';'
        cursor.execute(sql)
    for num in range(10, 100, 10):
        sql = 'TRUNCATE TABLE grades.sample_' + str(num) + ';'
        cursor.execute(sql)
    student_id = creat_balanced_student_id()
    student_id = list(student_id)
    for year, majors in zip(range(2017, 2021), major_list):
        print(year, majors)
        result = creat_score(student_id, year, majors)
        sql = 'INSERT INTO grades.transcript_balanced(id, major, term, score, year) VALUES (%s, %s, %s, %s, %s);'
        cursor.executemany(sql, result)
