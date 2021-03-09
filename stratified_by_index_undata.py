import pymysql
from pandas import DataFrame
from sqlalchemy import create_engine


if __name__ == '__main__':
    sample = 30000
    store_df = DataFrame([], columns=['id', 'index', 'time', 'score'])
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    # 清空表
    sql = 'TRUNCATE TABLE unknown_data.test_sample2;'
    cursor.execute(sql)
    sql = 'select * from unknown_data.data3 order by `index`;'
    cursor.execute(sql)
    result = cursor.fetchall()
    df_list = []
    print(1)
    t_df = DataFrame()
    ind = ''
    for res in result:
        Id = res[0]
        index = res[1]
        time = res[2]
        score = res[3]
        if ind == index:
            t_df.append([Id, index, time, score])
        else:
            df_list.append(t_df)
            t_df.drop(t_df.index, inplace=True)
        ind = index
    df_list.append(t_df)
    del df_list[0]
    print(2)
    df_length = len(df_list)
    avg = sample / df_length
    store_df = DataFrame()
    for df in df_list:
        if len(df) <= avg:
            store_df.append(df, ignore_index=True)
            df_length -= 1
            sample = sample - len(df)
            avg = sample / df_length
        else:
            df_sample = df.sample(frac=avg / len(df), replace=False, axis=0)
            store_df = store_df.append(df_sample, ignore_index=True)
            df_length -= 1
            sample = sample - len(df_sample)
            avg = sample / df_length
    store_df.to_sql('test_sample2', con=engine, if_exists='append', index=False, chunksize=100000)