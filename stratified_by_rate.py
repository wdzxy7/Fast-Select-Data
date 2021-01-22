import pymysql
from pandas import DataFrame
from sqlalchemy import create_engine


if __name__ == '__main__':
    sample = 3000000
    all_data = 36428644
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'SELECT COUNT(*), `index` FROM unknown_data.data3 GROUP BY `index` DESC;'
    cursor.execute(sql)
    result = cursor.fetchall()
    index_dict = {}
    for res in result:
        count = res[0]
        index = res[1]
        index_dict[index] = int(count)
    sql = 'SELECT * FROM unknown_data.data3 order BY `index` DESC, score ASC;'
    cursor.execute(sql)
    result = cursor.fetchall()
    df = DataFrame(result, columns=['id', 'index', 'time', 'score'])
    front = 0
    back = 0
    store_df = DataFrame([], columns=['id', 'index', 'time', 'score'])
    store_count = 0
    for key in index_dict.keys():
        data_sum = index_dict[key]
        max_range = front + data_sum - 1  # front最大值
        sample_length = data_sum / all_data * sample
        hist = int(data_sum / sample_length)
        back = front + hist - 1
        sam = 1 / hist  # 抽样率
        print(key, front, back)
        while front < max_range:
            data = df.loc[front:back, :]
            df_sample = data.sample(frac=sam, replace=False, axis=0)
            store_df = store_df.append(df_sample, ignore_index=True)
            front = back + 1
            back = back + hist
            if back > max_range:
                back = max_range
        if store_count == 50:
            store_df.to_sql('test_sample2', con=engine, if_exists='append', index=False, chunksize=100000)
            store_df.drop(store_df.index, inplace=True)
            store_count = 0
            continue
        store_count = store_count + 1
    store_df.to_sql('test_sample2', con=engine, if_exists='append', index=False, chunksize=100000)
