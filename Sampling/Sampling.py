#coding=utf-8
import numpy as np
import random
import time
import math

from pyspark import SparkContext
from pyspark import SparkConf
from loadData import loadData
from calcFalsePositive import FalsePositive

def setting(data):
    print("Datasize is %s"%len(data))
    t0=time.time()
    setList=set(data)
    t1=time.time()
    #print(setList)
    print("STANDARD size is: %s"%len(setList))
    print("STANDARD time is: %s s"%(t1-t0))

#抽样：随机抽样
#抽样方法：固定大小抽样
#该方法仅在size较小时适用，所有的数据都将被加载到driver内存中
def sizeFixed(rdd,size):
    print('----固定大小取样----')
    print('固定大小取样个数：%s'%size)
    all_size=rdd.count()
    mult=all_size/size
    sampleRDD=rdd.takeSample(False,size)
    sortRes=sorted(sampleRDD)
    temp = sortRes[0]
    cnt = 1
    for i in range(1, size):
        if (temp != sortRes[i]):
            cnt = cnt + 1
            temp = sortRes[i]
    estAll=math.floor(cnt*mult)
    all,fp=FalsePositive(all_size,estAll)
    err=(all_size-estAll)/all_size
    print('抽样中集合基数为：%s'%cnt)
    print('由样本估计总体集合基数为：%s'%estAll)
    print('误差率为： %s'%(fp*100)+'%')

#抽样方法：固定比例抽样
def rateFixed(rdd,rate):
    print('----固定比例抽样----')
    print('固定比例为：%s' % rate)
    all_size = rdd.count()
    size=math.floor(all_size*rate)
    mult = all_size / size
    sampleRDD=rdd.sample(False,rate)


    #！！！注意！！！当内存中装不下数据集时使用.RDD自动分配到内存+磁盘中
    # sortedRDD=sampleRDD.map(lambda x: (x,x)).sortByKey()
    # newRDD=sortedRDD.filter(lambda x: x)

    # ！！！注意！！！当内存能装得下数据时候使用
    sortRes = sorted(sampleRDD.collect())
    temp = sortRes[0]
    cnt = 1
    for i in range(1, size):
        if (temp != sortRes[i]):
            cnt = cnt + 1
            temp = sortRes[i]
    estAll = math.floor(cnt * mult)
    all, fp = FalsePositive(all_size, estAll)

    print('抽样中集合基数为：%s' % cnt)
    print('由样本估计总体集合基数为：%s' % estAll)
    print('误差率为： %s' % (fp * 100) + '%')

if __name__ == '__main__':
    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    sc.setLogLevel("WARN")
    rdd1 = sc.textFile("file:///home/evan/PycharmProjects/BIg_Data_lab/text_3.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(',')).map(lambda x: int(x))
    rdd=rdd2

    time1=time.time()
    # sizeFixed(rdd,size=100000)
    rateFixed(rdd,rate=0.2)
    time2=time.time()
    print("Time is :%s"%(time2-time1)+'s')
