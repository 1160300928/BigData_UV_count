#coding=utf-8
import numpy as np
import random
import time

from math import sqrt,floor
from pyspark import SparkContext
from pyspark import SparkConf
from loadData import loadData
from calcFalsePositive import FalsePositive
from collections import Counter

#自适应大小抽样法：
#b:查询（抽样）q的大小界限
#d:error ratio:按照题意要求的错误的比率(0<=d<1)小数形式
#p:置信度(0<=p<1)
def adaptiveSampling(b, d, p):
    S=0
    m=0
    # n=random.randint(0,b)
    # |RandomSample(q)|
    n=351

    while S< b*d*(d+1)/(1-sqrt(p)):
        S=S+n
        m=m+1
        # print(S,m)
    size=round(n*S/m)
    return size

def sequential(rdd,error_rate,size):
    all_size=rdd.count()
    new_error=1.0
    cnt=0

    n=round(all_size/100)
    # sampleRDD = rdd.takeSample(False, size)
    while new_error>error_rate:
        mult = all_size / size
        sampleRDD=rdd.take(size)
        sortRes = sorted(sampleRDD)
        temp = sortRes[0]
        cnt = 1
        for i in range(1, size):
            if (temp != sortRes[i]):
                cnt = cnt + 1
                temp = sortRes[i]
        estAll = floor(cnt * mult)
        all, fp = FalsePositive(all_size, estAll)
        new_error = fp
        # print("当前FP率为：%s"%fp)
        size=size+n
    print('抽样中集合基数为：%s' % cnt)
    print('Sequential由样本估计总体集合基数为：%s' %estAll)
    print('Sequential最终误差率为： %s' % (new_error * 100) + '%')


#p为置信度
def sequentialSampling(rdd, p):
    all_size = rdd.count()
    n = round(all_size / 100) #初始值
    zp=(1+p)/2
    M=0
    r=0 #抽样大小的估计
    sampleRDD = rdd.take(n)
    mu=np.mean(sampleRDD) #均值
    sigma=np.var(sampleRDD) #方差
    while sigma<=0 or r<zp*sqrt(sigma)/(sqrt(n)):
        sigma=sigma+(M-(n-1)*r)/(n*(n-1))
        M=M+r
        r=M/n
        sigma=sigma/(n-1)

    return r


if __name__ == '__main__':
    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    sc.setLogLevel("WARN")
    rdd1 = sc.textFile("file:///home/evan/PycharmProjects/BIg_Data_lab/text_3.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(',')).map(lambda x: int(x))
    rdd=rdd2

    b=rdd2.count()
    d=0.001
    p=0.95

    time1=time.time()
    size=adaptiveSampling(b,d,p)
    print("Adaptive size:%s"%size)
    sequential(rdd,d,size)
    time2=time.time()
    print("----time is:%s"%(time2-time1)+'s----')