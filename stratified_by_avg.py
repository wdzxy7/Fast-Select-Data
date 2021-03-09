import pymysql
from pandas import DataFrame
from sqlalchemy import create_engine


if __name__ == '__main__':
    sample = 30000
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    # 清空表
    sql = 'TRUNCATE TABLE unknown_data.test_sample2;'
    cursor.execute(sql)
    sql = 'select count(*) from unknown_data.data3 order by `index`;'
    cursor.execute(sql)
    result = cursor.fetchall()
    data_sum = result[0][0]
    print(data_sum)
    sql = 'select count(distinct `index`) from unknown_data.data3;'
    cursor.execute(sql)
    result = cursor.fetchall()
    index_sum = result[0][0]
    print(index_sum)
    hist_avg = int(int(data_sum) / int(index_sum))  # 一个index多少个数据平均
    sample_avg = int(sample / int(index_sum)) + 1  # 分层抽样每一层抽多少数据
    front = 0
    back = hist_avg
    sample_rate = sample_avg / hist_avg
    print(sample_avg, hist_avg, sample_rate)
    sql = 'select * from unknown_data.data3 order by `index`;'
    cursor.execute(sql)
    result = cursor.fetchall()
    df = DataFrame(result, columns=['id', 'index', 'time', 'score'])
    store_df = DataFrame([], columns=['id', 'index', 'time', 'score'])
    while front < data_sum:
        print(front, back)
        sam = df.loc[front:back, :]
        df_sample = sam.sample(frac=sample_rate, replace=False, axis=0)
        store_df = store_df.append(df_sample, ignore_index=True)
        front = front + hist_avg
        back = back + hist_avg
        if back > data_sum:
            back = data_sum
    store_df.to_sql('test_sample2', con=engine, if_exists='append', index=False, chunksize=100000)