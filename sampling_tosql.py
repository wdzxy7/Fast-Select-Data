import pymysql
from pandas import Series, DataFrame
import time
from sqlalchemy import create_engine
import pandas as pd
from pandas.api.types import CategoricalDtype
import fsspec


if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://root:@localhost:3308/grades', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    '''
    for sample_num in range(1, 12):
        break
        sql = 'TRUNCATE TABLE grades.sample' + str(sample_num) + ';'
        cursor.execute(sql)
    '''
    time.process_time()
    t1 = time.process_time()
    print(t1)
    sql = 'select * from transcript_balanced;'
    data = pd.read_sql(sql, con=engine)
    print(data)
    df = DataFrame(data, columns=['id', 'major', 'term', 'score', 'year'])
    sams = [0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
    for number in range(6):
        sam = sams[number]
        table_name = 'sample_' + str(sam * 100)
        print(table_name)
        df_sample = df.sample(frac=sam, replace=False, axis=0)
        df_sample.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=100000)
    print(t1)