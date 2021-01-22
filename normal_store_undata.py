import os
import time
import pymysql


def zip_data():
    small_data[0] = name
    small_data[2] = small_data[2].replace('.000', '')
    t = time.localtime(float(small_data[2]))
    t = time.strftime("%Y-%m-%d %H:%M:%S", t)
    ind = small_data[1]
    front = ind[0:16]
    back = ind[-12:-8]
    ind = front + back
    small_data[2] = t
    small_data[1] = ind
    data = tuple(small_data)
    print(data)
    time.sleep(2000)
    store.append(data)


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='unknown_data', charset='utf8')
    cursor = connect.cursor()
    sql = 'INSERT INTO unknown_data.data(id, `index`, time, score) VALUES (%s,%s,%s,%s);'
    files = []
    for root, dirs, file in os.walk('./data/all_data'):
        files = file
    file_dict = {}
    num = 0
    s = 1
    name = ''
    store = []
    length = len(files)
    for file_name in files:
        print(length, s)
        s += 1
        store.clear()
        file = file_name.split('-')
        file = file[0]
        try:
            name = file_dict[file]
        except KeyError:
            name = num
            file_dict[file] = num
            num = num + 1
        print(file_dict)
        path = './data/all_data/' + file_name
        with open(path, 'r') as f:
            txt = f.read()
        txt = txt.split(']')
        for small_data in txt:
            small_data = small_data.replace('[', '').replace('\"', '')
            small_data = small_data.split(',')
            del small_data[0]
            if len(small_data) < 4:
                continue
            else:
                zip_data()
        # cursor.executemany(sql, store)
