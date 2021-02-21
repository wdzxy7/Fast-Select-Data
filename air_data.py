from sqlalchemy import create_engine
import single_test as st
import pymysql
from pandas import DataFrame
import sql_connect


sample_sum = 1500
c = 'US'
p = 'pm1'
Eps = 0.002
MinPts = 8


def dbscan_test():
    print('DBSCAN START')
    sql_con = sql_connect.sql_c()
    '''
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    '''
    database = 'dbscan_result'
    sql = 'TRUNCATE TABLE unknown_data.dbscan_result;'
    sql_con.cursor.execute(sql)
    # sql = 'select value, count(value) from unknown_data.air WHERE parameter = \'' + p + '\' and country = \'' + c + '\' group by value;'
    sql = 'select score, count(score) from unknown_data.data3 where `index`=22021001101410011321 group by score;'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    layer = st.DBSCAN(same_data, Eps=Eps, MinPts=MinPts)
    for i in layer:
        print(i)
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    # sql = 'SELECT value FROM unknown_data.air WHERE parameter = \'' + p + '\' and country = \'' + c + '\';'
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    sql_con.cursor.execute(sql)
    sql_result = sql_con.cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    df_list, zero_data = st.spilt_data_by_layer(layer, df)
    data_sum = len(sql_result)
    st.avg_sampling_data(engine, df_list, data_sum, sample_sum, zero_data, database)


def optics_test():
    print('OPTICS START')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    database = 'optics_result'
    sql = 'TRUNCATE TABLE unknown_data.' + database + ';'
    cursor.execute(sql)
    sql = 'SELECT value, count(value) FROM unknown_data.air WHERE parameter = \'' + p + '\' and country = \'' + c + '\' group by value;'
    # sql = 'select score, count(score) from unknown_data.data3 where `index`=22021001101410011321 group by score;'
    cursor.execute(sql)
    res = cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    layer = st.OPTICS(same_data, Eps=Eps, MinPts=MinPts)
    for i in layer:
        print(i)
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    # sql = 'SELECT value FROM unknown_data.air WHERE parameter = \'' + p + '\' and country = \'' + c + '\';'
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    df_list, zero_data = st.spilt_data_by_layer(layer, df)
    data_sum = len(sql_result)
    st.avg_sampling_data(engine, df_list, data_sum, sample_sum, zero_data, database)


def random_test():
    print('RANDOM START')
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.random_result;'
    cursor.execute(sql)
    # sql = 'SELECT value FROM unknown_data.air WHERE parameter = \'' + p + '\' and country = \'' + c + '\';'
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    df_sample = df.sample(frac=sample_sum / len(df), replace=False, axis=0)
    df_sample.to_sql('random_result', con=engine, if_exists='append', index=False, chunksize=100000)


def avg_layer_test():
    print('AVG START')
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.avg_result;'
    cursor.execute(sql)
    # sql = 'SELECT value FROM unknown_data.air WHERE parameter = \'' + p + '\' and country = \'' + c + '\' order by value;'
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    front = 0
    back = 0
    data_sum = len(df)
    max_range = front + data_sum - 1
    hist = int(data_sum / sample_sum)
    sam = 1 / hist
    store_df = DataFrame([], columns=['score']).astype('float')
    while front < max_range:
        data = df.loc[front:back, :]
        df_sample = data.sample(frac=sam, replace=False, axis=0)
        store_df = store_df.append(df_sample, ignore_index=True)
        front = back + 1
        back = back + hist
        if back > max_range:
            back = max_range
    store_df.to_sql('avg_result', con=engine, if_exists='append', index=False, chunksize=100000)


if __name__ == '__main__':
    dbscan_test()
    '''
    optics_test()
    random_test()
    avg_layer_test()
    '''