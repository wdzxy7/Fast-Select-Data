import pymysql
import time
import xlutils.copy
from openpyxl import Workbook
import xlrd


def deep_test():
    sql = 'select major,avg(score) from grades.transcript_balanced where term = \'1\' group by major;'
    sql1 = 'Select major,avg(score) from grades.sample1 where term = \'1\' group by major;'
    sql2 = 'Select major,avg(score) from grades.sample2 where term = \'1\' group by major;'
    sql3 = 'Select major,avg(score) from grades.sample3 where term = \'1\' group by major;'
    sql4 = 'Select major,avg(score) from grades.sample4 where term = \'1\' group by major;'
    sql5 = 'Select major,avg(score) from grades.sample5 where term = \'1\' group by major;'
    sql6 = 'Select major,avg(score) from grades.sample6 where term = \'1\' group by major;'
    sql7 = 'Select major,avg(score) from grades.sample7 where term = \'1\' group by major;'
    sql8 = 'Select major,avg(score) from grades.sample8 where term = \'1\' group by major;'
    sql9 = 'Select major,avg(score) from grades.sample9 where term = \'1\' group by major;'
    sql10 = 'Select major,avg(score) from grades.sample10 where term = \'1\' group by major;'
    sql11 = 'Select major,avg(score) from grades.sample11 where term = \'1\' group by major;'
    sql12 = 'Select major,avg(score) from grades.sample12 where term = \'1\' group by major;'
    sql13 = 'Select major,avg(score) from grades.sample13 where term = \'1\' group by major;'
    sql14 = 'Select major,avg(score) from grades.sample14 where term = \'1\' group by major;'
    sql15 = 'Select major,avg(score) from grades.sample15 where term = \'1\' group by major;'
    sql16 = 'Select major,avg(score) from grades.sample16 where term = \'1\' group by major;'
    sql17 = 'Select major,avg(score) from grades.sample17 where term = \'1\' group by major;'
    sql18 = 'Select major,avg(score) from grades.sample18 where term = \'1\' group by major;'
    sql19 = 'Select major,avg(score) from grades.sample19 where term = \'1\' group by major;'
    sql20 = 'Select major,avg(score) from grades.sample20 where term = \'1\' group by major;'
    time_dict = {0: [], 1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: [], 9: [], 10: [], 11: [],
                 12: [], 13: [], 14: [], 15: [], 16: [], 17: [], 18: [], 19: [], 20: []}
    sql_list = [sql, sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8, sql9, sql10, sql11, sql12, sql13, sql14,
                sql15, sql16, sql17, sql18, sql19, sql20]
    return time_dict, sql_list


def sample_test():
    sql = 'select major,avg(score) from grades.transcript_balanced where term = \'1\' group by major;'
    sql1 = 'Select major,avg(score) from grades.sample_90 where term = \'1\' group by major;'
    sql2 = 'Select major,avg(score) from grades.sample_80 where term = \'1\' group by major;'
    sql3 = 'Select major,avg(score) from grades.sample_70 where term = \'1\' group by major;'
    sql4 = 'Select major,avg(score) from grades.sample_60 where term = \'1\' group by major;'
    sql5 = 'Select major,avg(score) from grades.sample_50 where term = \'1\' group by major;'
    sql6 = 'Select major,avg(score) from grades.sample_40 where term = \'1\' group by major;'
    sql7 = 'Select major,avg(score) from grades.sample_30 where term = \'1\' group by major;'
    sql8 = 'Select major,avg(score) from grades.sample_20 where term = \'1\' group by major;'
    sql9 = 'Select major,avg(score) from grades.sample_10 where term = \'1\' group by major;'
    time_dict = {0: [], 1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: [], 9: []}
    sql_list = [sql, sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8, sql9]
    return sql_list, time_dict


if __name__ == '__main__':
    # path = 'test_result.xls'
    path = 'deep_test_result.xls'
    data = xlrd.open_workbook(path)
    sheet = data.sheet_by_name("time_cost")
    rows = sheet.nrows
    ws = xlutils.copy.copy(data)
    table = ws.get_sheet(0)
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    t1 = time.perf_counter()
    time_dict, sql_list = deep_test()
    # sql_list, time_dict = sample_test()
    for i in range(50):
        number = i % len(time_dict)
        if number == 0:
            continue
        test_sql = sql_list[number]
        t1 = time.perf_counter()
        cursor.execute(test_sql)
        t2 = time.perf_counter()
        use_time = t2 - t1
        time_dict[i % len(time_dict)].append(use_time)
        print(test_sql)
        print('----------------------------' + str(i) + '----------------------------')
    for i in range(1, len(time_dict)):
        t_list = time_dict[i]
        write_row = rows
        for t in t_list:
            table.write(write_row, i, t)
            write_row += 1
    ws.save(path)
