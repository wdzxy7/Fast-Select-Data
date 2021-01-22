import os
import time
import ctypes
import pymysql
from multiprocessing import Process
from multiprocessing import Lock
import multiprocessing as mp


class Run(Process):
    def __init__(self, file_list, f_dict, run_num, executed_num, f_sum, s_lock, p_name):
        super().__init__()
        self.files = file_list
        self.file_dict = f_dict
        self.num = run_num
        self.executed = executed_num
        self.file_sum = f_sum
        self.lock = s_lock
        self.p_name = p_name

    def run(self):
        db = int(self.p_name.replace('process', '').replace('.0', ''))
        sql = 'INSERT INTO unknown_data.data' + str(db) + '(id, `index`, time, score) VALUES (%s,%s,%s,%s);'
        connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='unknown_data',
                                  charset='utf8')
        cursor = connect.cursor()
        dele = 'TRUNCATE TABLE unknown_data.data' + str(db) + ';'
        cursor.execute(dele)
        for file_name in self.files:
            file = file_name.split('-')
            file = file[0]
            self.lock.acquire()
            try:
                name = self.file_dict[file]
                self.executed.value = self.executed.value + 1
            except Exception as e:
                self.executed.value = self.executed.value + 1
                name = self.num.value
                self.file_dict[file] = self.num.value
                self.num.value = self.num.value + 1
            print(self.p_name, file, name, self.file_sum, self.executed.value)
            self.lock.release()
            path = './data/all_data/' + file_name
            with open(path, 'r') as f:
                txt = f.read()
            txt = txt.split(']')
            data = self.split_txt(txt, name)
            cursor.executemany(sql, data)
        cursor.close()

    def split_txt(self, txt, name):
        datas = []
        for small_data in txt:
            small_data = small_data.replace('[', '').replace('\"', '')
            small_data = small_data.split(',')
            del small_data[0]
            if len(small_data) < 4:
                continue
            else:
                data = self.zip_data(name, small_data)
                try:
                    float(small_data[3])
                except:
                    continue
                if abs(float(small_data[3])) > 9999999:
                    continue
                datas.append(data)
        return datas

    @staticmethod
    def zip_data(name, small_data):
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
        return data


def main():
    global file_sum
    files = []
    for root, dirs, file in os.walk('./data/all_data'):
        files = file
    file_sum = len(files)
    front = 0
    back = 249
    process_name = []
    file_list = []
    count = 1
    while back <= 12949:
        process_name.append('process' + str(count))
        count = count + 1
        t_file = files[front:back]
        file_list.append(t_file)
        if back == 12949:
            break
        front = front + 250
        back = back + 250
        if back > 12949:
            back = 12949
    process_list = []
    print(process_name)
    for file, p_name in zip(file_list, process_name):
        p = Run(file, file_dict, num, executed, file_sum, lock, p_name)
        process_list.append(p)
    for p in process_list:
        p.start()
    for p in process_list:
        p.join()


if __name__ == '__main__':
    t1 = time.perf_counter()
    file_dict = mp.Manager().dict()
    num = mp.Value('l', 0)
    executed = mp.Value('l', 0)
    file_sum = mp.Value('l', 0)
    lock = mp.Manager().Lock()
    main()
    print(file_dict)
    t2 = time.perf_counter()
    use_time = t2 - t1
    print(use_time)