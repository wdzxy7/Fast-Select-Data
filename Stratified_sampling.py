import time
from decimal import Decimal
import pymysql
from pandas import DataFrame


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    for sample_num in range(1, 21):
        sql = 'TRUNCATE TABLE grades.stratified_sample' + str(sample_num) + ';'
        cursor.execute(sql)
    sql = 'select * from grades.exponential_data order by score;'
    cursor.execute(sql)
    res = cursor.fetchall()
    df = DataFrame(res)
    time_spend = []
    for i in range(1, 21):
        front = 0
        back = 79999
        t1 = time.perf_counter()
        while front < 30000000:
            sam = df.loc[front:back, :]
            df_sample = sam.sample(frac=0.00005 * i, replace=False, axis=0)
            store_list = []
            for mes in df_sample.itertuples():
                dt = list(mes)
                del dt[0]
                store_list.append(tuple(dt))
            sql = 'INSERT INTO grades.stratified_sample' + str(i) + '(student, major, term, score, year) VALUES (%s, %s, %s, %s, %s);'
            cursor.executemany(sql, store_list)
            front = front + 80000
            back = back + 80000
        t2 = time.perf_counter()
        use_time = t2 - t1
        print(use_time)
        time_spend.append(use_time)
    dic = {}
    for i in range(len(time_spend)):
        dic[i] = []
        dic[i].append(time_spend[i])
    time_df = DataFrame(dic)
    sam = Decimal('0.00005')
    c = Decimal('0.00005')
    ind = []
    for i in range(len(time_spend)):
        ind.append(str(sam) + '%')
        sam = sam + c
    time_df.columns = ind
    time_df.to_csv('creat_time_tempfile.csv')