import threading
import pymysql
from pandas import Series, DataFrame
import time

connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
cursor = connect.cursor()


class Sampling(threading.Thread):
    def __init__(self, df_sample, sam):
        threading.Thread.__init__(self)
        self.df = df_sample
        self.sam = sam

    def run(self) -> None:
        global cursor
        store_list = []
        for mes in self.df.itertuples():
            dt = list(mes)
            del dt[0]
            store_list.append(tuple(dt))
        number = 10 - int(self.sam*10)
        sql = 'INSERT INTO grades.sample' + str(number) + '(id, major, term, score) VALUES (%s, %s, %s, %s);'
        print(sql)
        cursor.executemany(sql, store_list)


if __name__ == '__main__':
    for sample_num in range(1, 10):
        sql = 'TRUNCATE TABLE grades.sample' + str(sample_num) + ';'
        cursor.execute(sql)
    sql = 'select * from grades.transcript_balanced;'
    cursor.execute(sql)
    result = cursor.fetchall()
    df = DataFrame(result, columns=['id', 'major', 'term', 'score'])
    sams = [0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
    ts = []
    for sam in sams:
        df_sample = df.sample(frac=sam, replace=False, axis=0)
        sample = Sampling(df_sample, sam)
        ts.append(sample)
    for t in ts:
        t.start()
    for t in ts:
        t.join()