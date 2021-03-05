import single_test as st
import sql_connect


if __name__ == '__main__':
    eps = 0.1
    minpts = 10
    sql = 'select avg(value), count(value) from unknown_data.air where locationId=63094 group by value;'
    sql_con = sql_connect.Sql_c()
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    dbscan_layer = st.DBSCAN(same_data, Eps=eps, MinPts=minpts)
    for i in dbscan_layer:
        print(sorted(i))