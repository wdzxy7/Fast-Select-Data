import single_test as st
import pymysql
from pandas import DataFrame
from sqlalchemy import create_engine
import sql_connect


if __name__ == '__main__':
    sql_con = sql_connect.Sql_c()
    sql2 = 'TRUNCATE TABLE unknown_data.dbscan_result;'
    sql6 = 'TRUNCATE TABLE unknown_data.avg_dbscan_result;'
    clear_sql = [sql2, sql6]
    t_sql3 = 'select avg(score) from unknown_data.dbscan_result;'
    t_sql4 = 'select avg(score) from unknown_data.avg_dbscan_result;'
    test_sql = [t_sql3, t_sql4]
    sql = 'SELECT `value`, count(`value`) FROM unknown_data.air WHERE locationId=63094 GROUP BY `value`;'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    print(same_data)
    dbscan_layer = st.DBSCAN(same_data, Eps=0.11, MinPts=10)
    for i in dbscan_layer:
        print(sorted(i))

    sql = 'SELECT `value` FROM unknown_data.air WHERE locationId=63094;'
    sql_con.cursor.execute(sql)
    sql_result = sql_con.cursor.fetchall()
    data_sum = len(sql_result)
    data = DataFrame(sql_result, columns=['score']).astype('float')
    dbdata, zero_data = st.spilt_data_by_layer(dbscan_layer, data)
    stand = 0.017775
    t_list = []
    test_result = []
    ind = []
    for sample_sum in range(5000, 20000, 500):
        t_list.clear()
        print(sample_sum)
        ind.append(sample_sum)
        for sql in clear_sql:
            sql_con.cursor.execute(sql)
        st.sampling_data(sql_con.engine, dbdata, data_sum, sample_sum, zero_data, 'dbscan_result')
        st.avg_sampling_data(sql_con.engine, dbdata, data_sum, sample_sum, zero_data, 'avg_dbscan_result')
        for test in test_sql:
            sql_con.cursor.execute(test)
            result = sql_con.cursor.fetchall()
            avg = round(float(result[0][0]), 6)
            accuracy = round(abs(avg - stand) / stand * 100, 6)
            t_list.append(accuracy)
        t = t_list.copy()
        t = tuple(t)
        test_result.append(t)
    df = DataFrame(test_result, columns=['DBSCAN', 'avg_DBSCAN'])
    df.index = ind
    print(df)
    path = 'air_Clustering_accuracy.csv'
    df.to_csv(path)