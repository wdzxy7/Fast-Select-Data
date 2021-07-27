'''
早期测试代码，对自己创造的数据抽样
'''
from decimal import Decimal
import pymysql
from pandas import DataFrame
import time


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    # 清空表
    '''
    for sample_num in range(1, 21):
        sql = 'TRUNCATE TABLE grades.poisson_sample' + str(sample_num) + ';'
        cursor.execute(sql)
    '''
    time.process_time()
    # 查询总数据
    sql = 'select * from grades.poisson_data;'
    cursor.execute(sql)
    result = cursor.fetchall()
    t1 = time.process_time()
    print(t1)
    df = DataFrame(result, columns=['id', 'major', 'term', 'score', 'year'])
    time_spend = []
    sam = 0.00005
    # 抽样
    for number in range(1, 21):
        if sam == 1:
            break
        table_name = 'sample' + str(number)
        t1 = time.perf_counter()
        df_sample = df.sample(frac=sam, replace=False, axis=0)
        store_list = []
        for mes in df_sample.itertuples():
            dt = list(mes)
            del dt[0]
            store_list.append(tuple(dt))
        sql = 'INSERT INTO grades.poisson_sample' + str(number) + '(student, major, term, score, year) VALUES (%s, %s, %s, %s, %s);'
        print(sql)
        cursor.executemany(sql, store_list)
        t2 = time.perf_counter()
        use_time = t2 - t1
        print(use_time)
        time_spend.append(use_time)
        sam = sam + 0.00005
    dic = {}
    print(time_spend)
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
    print(time_df)
    time_df.columns = ind
    print(time_df)
    time_df.to_csv('creat_time_tempfile.csv')
    print(t1)