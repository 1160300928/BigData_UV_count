#coding: utf-8
import numpy as np
from sklearn.utils import shuffle
import random
import time
from pyspark import SparkContext
from pyspark import SparkConf

#写入到文件中
def write_in_file(data,file):
    filePath=file
    f = open(filePath, 'w')
    # time1 = time.time()
    # size=np.shape(data)[0]
    # for i in range(size-1):
    #     f.write(str(data[i])+'\n')
    f.write(str(data))
    # time2 = time.time()
    # print("生成文件用时:%s"%(time2 - time1))
    #print("生成文件大小：%s"%((os.path.getsize(filePath))/(float(1024*1024*1024)))+'GB')
    f.close()

def change(fw,fr):
    with open(fw,mode='w') as fw, open(fr,mode='r') as fr:
        line=fr.readline()
        line=line.strip(']').strip('[')
        fw.write(line)

#数据生成：生成一个大小为n的multisets。其中，重复元素的个数以及重复的次数为随机分布。
def generate(begin,end):
    random.seed(20)
    random_num=np.random.randint(1,10000)
    data=np.linspace(begin,end,end,dtype='int32')
    shuffle_data=shuffle(data)

    for i in range(random_num):
        shuffle_data=list(shuffle_data)
        random_num=random.randint(begin,end)
        random_th=random.randint(begin,end)
        frequence = random.randint(1,100)

        for j in range(frequence):
            shuffle_data.insert(random_th,random_num)
    return shuffle_data


if __name__ == '__main__':
    begin=1
    end=100000
    fr='data_s2.txt'
    fw='text_s2.txt'
    gw='data_s2.txt'
    change(fw,fr)
    # data=generate(begin,end)
    # write_in_file(data,gw)


