from sqlalchemy import create_engine
import single_test as st
import pymysql
from pandas import DataFrame


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    database = 'optics_result'
    sql = 'TRUNCATE TABLE unknown_data.' + database + ';'
    cursor.execute(sql)
    sql = 'select value, count(value) from unknown_data.single_data where parameter=\'pm1\' group by value;'
    cursor.execute(sql)
    res = cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    layer = st.OPTICS(same_data, Eps=0.2, MinPts=10)


    sample_sum = 788
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    sql = 'select value from unknown_data.single_data where parameter=\'pm1\';'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    df_list, zero_data = st.spilt_data_by_layer(layer, df)
    data_sum = len(sql_result)
    st.sampling_data(engine, df_list, data_sum, sample_sum, zero_data, database)