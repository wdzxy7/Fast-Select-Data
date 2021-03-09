import time
import pymysql
from openpyxl import Workbook


def test1():
    wb = Workbook()
    excel = wb.active
    excel['A1'] = 'index'
    excel['B1'] = 'stand'
    excel['C1'] = 'sample'
    excel['D1'] = 'accuracy'
    count = 2
    res_dict = {}
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'select `index`, avg(score) from unknown_data.data3 group by `index`;'
    cursor.execute(sql)
    res = cursor.fetchall()
    for i in res:
        index = i[0]
        avg = i[1]
        res_dict[index] = []
        res_dict[index].append(avg)
    sql = 'select `index`, avg(score) from unknown_data.test_sample2 group by `index`;'
    cursor.execute(sql)
    res = cursor.fetchall()
    c = len(res)
    for i in res:
        index = i[0]
        avg = i[1]
        try:
            res_dict[index].append(avg)
        except:
            res_dict[index] = []
            res_dict[index].append(avg)
    s = 0
    for key in res_dict.keys():
        stand = res_dict[key][0]
        sample = res_dict[key][1]
        try:
            accuracy = round(abs(stand - sample) / abs(stand), 10) * 100
        except:
            accuracy = 0
        res_dict[key].append(accuracy)
    print(res_dict['21021002101810011121'])
    print(res_dict['21021002102410031121'])
    print(res_dict['21021002103410031121'])
    res_dict = sorted(res_dict.items(), key=lambda x: x[1][2], reverse=True)
    accuracy_dict = {}
    for i in res_dict:
        accuracy_dict[i[0]] = list(i[1])
    for key in accuracy_dict.keys():
        stand = accuracy_dict[key][0]
        sample = accuracy_dict[key][1]
        accuracy = accuracy_dict[key][2]

        excel['A' + str(count)] = key
        excel['B' + str(count)] = stand
        excel['C' + str(count)] = sample
        excel['D' + str(count)] = str(accuracy) + '%'
        count = count + 1
    wb.save('undata_accuracy_data.xlsx')


def test2():
    wb = Workbook()
    excel = wb.active
    excel['A1'] = 'index'
    excel['B1'] = 'stand'
    excel['C1'] = 'sample'
    excel['D1'] = 'accuracy'
    count = 2
    res_dict = {}
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'select distinct `index` from unknown_data.test_sample2;'
    cursor.execute(sql)
    res = cursor.fetchall()
    indexs = []
    for i in res:
        indexs.append(i[0])
    c = 1
    for ind in indexs:
        print(c)
        c += 1
        sql = 'select avg(score) from unknown_data.data3 where `index`=' + str(ind) + ';'
        cursor.execute(sql)
        res = cursor.fetchall()
        stand = float(res[0][0])
        sql = 'select avg(score) from unknown_data.test_sample2 where `index`=' + str(ind) + ';'
        cursor.execute(sql)
        res = cursor.fetchall()
        sample = float(res[0][0])
        accuracy = round(abs(stand - sample) / abs(stand), 10) * 100
        res_dict[ind] = [stand, sample, accuracy]
        time.sleep(0.1)
    res_dict = sorted(res_dict.items(), key=lambda x: x[1][2], reverse=True)
    accuracy_dict = {}
    for i in res_dict:
        accuracy_dict[i[0]] = list(i[1])
    for key in accuracy_dict.keys():
        stand = accuracy_dict[key][0]
        sample = accuracy_dict[key][1]
        accuracy = accuracy_dict[key][2]

        excel['A' + str(count)] = key
        excel['B' + str(count)] = stand
        excel['C' + str(count)] = sample
        excel['D' + str(count)] = str(accuracy) + '%'
        count = count + 1
    wb.save('undata_accuracy_data.xlsx')


if __name__ == '__main__':
    test2()