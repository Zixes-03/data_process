import mars.dataframe as md
import pandas as pd
from datetime import datetime
import time
from multiprocessing import Pool
import os

def process_error_data_01(data):
    for index, row in data.iterrows():
        if row['ONTIME'] != row['WORKTIME'] + row['STOPTIME']:
            # print(index, row['MACHINE_ID'] ,row['PDLINE_ID'])
            data.drop(index = index, axis=0, inplace=True)
    return data

# https://blog.csdn.net/qq_18254385/article/details/90401181

if __name__ == '__main__':  # 没有这行会错误

    original_data = md.read_csv(
    'F:/Projects/Python/data_process/csv/splited_data/data_tianzheng_assembly_2.csv'
                                ).to_pandas()
    original_data.index = pd.DatetimeIndex(original_data["UPDATE_DATE"])
    data_len = original_data.shape[0]
    print(data_len)

    data_1 = original_data[:(data_len * 1) // 8]
    data_2 = original_data[(data_len * 1) // 8:(data_len * 2) // 8]
    data_3 = original_data[(data_len * 2) // 8:(data_len * 3) // 8]
    data_4 = original_data[(data_len * 3) // 8:(data_len * 4) // 8]
    data_5 = original_data[(data_len * 4) // 8:(data_len * 5) // 8]
    data_6 = original_data[(data_len * 5) // 8:(data_len * 6) // 8]
    data_7 = original_data[(data_len * 6) // 8:(data_len * 7) // 8]
    data_8 = original_data[(data_len * 7) // 8:]

    data_list = [data_1, data_2, data_3, data_4, data_5, data_6, data_7, data_8]

    start = time.time()

    po = Pool()  # 创建一个进程池
    # map_async只适用于单变量输入函数
    data_processed = po.map_async(process_error_data_01, data_list)
    # https://blog.csdn.net/u011331731/article/details/106320199/
    po.close()  # 关闭进程池
    po.join()  # 等待进程池内的所有子进程完毕
    # https://ask.csdn.net/questions/1058603?sort=id
    # https://blog.csdn.net/weixin_37111106/article/details/85122988
    # https://blog.csdn.net/u013176681/article/details/77340320
    data_processed = data_processed.get()

    end = time.time()
    print('Processing time: %0.2f seconds.' % (end - start))

    original_data_processed = pd.concat(data_processed, axis=0)
    print(original_data_processed.shape[0])
