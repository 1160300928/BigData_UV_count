#coding=utf-8
import numpy as np
import random
import time
import operator

from math import sqrt,floor,log
from pyspark import SparkContext
from pyspark import SparkConf
from loadData import loadData
from collections import Counter
from calcFalsePositive import FalsePositive

#自适应大小抽样法：
#b:查询（抽样）q的大小界限
#d:error ratio:按照题意要求的错误的比率(0<=d<1)小数形式
#p:置信度(0<=p<1)
def adaptiveSampling(b, d, p): 
    S=0
    m=0
    # n=random.randint(0,b)
    # |RandomSample(q)|
    n=511
    while S< b*d*(d+1)/(1-sqrt(p)):
        S=S+n
        m=m+1
        # print(S,m)
    size=floor(n*S/m)
    return size

#Guaranteed-Error Estimator
def GEE(rdd,size):
    all_size=rdd.count()
    print('集合所有元素个数为：%s'%all_size)
    f = np.array(np.zeros(size,dtype='int32'))

    sampleRDD=rdd.takeSample(False, size)
    dict=Counter(sampleRDD) # 转化成字典 格式为： (key:出现的次数)
    values=list(dict.values())
    #计算重复i次的元素个数fi（0<=i<=r）
    for i in range(size):
        cnt=0
        for j in range(len(values)):
            if(values[j]==i+1):
                cnt=cnt+1
        f[i]=cnt
    #GEE估计
    sum=0
    for i in range(1,size):
        sum+=f[i]
    D=sqrt(all_size/size)*f[0]+sum

    print(round(D)*2)

def normal(rdd,size):
    all_size = rdd.count()
    mult = all_size / size
    sampleRDD = rdd.takeSample(False, size)
    sortRes = sorted(sampleRDD)

    temp = sortRes[0]
    cnt = 1
    for i in range(1, size):
        if (temp != sortRes[i]):
            cnt = cnt + 1
            temp = sortRes[i]
    estAll = floor(cnt * mult)
    all, fp = FalsePositive(all_size, estAll)
    err = (all_size - estAll) / all_size
    print('抽样中集合基数为：%s' % cnt)
    print('由样本估计总体集合基数为：%s' % estAll)
    print('误差率为： %s' % (fp * 100) + '%')

if __name__ == '__main__':
    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    sc.setLogLevel("WARN")
    rdd1 = sc.textFile("file:///home/evan/PycharmProjects/BIg_Data_lab/text_3.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(',')).map(lambda x: int(x))

    b=100000
    d=0.001
    p=0.95

    time1=time.time()
    size=adaptiveSampling(b,d,p)
    print('Adaptive Size:%s'%size)
    # GEE(rdd2,size)
    normal(rdd2,size)
    time2=time.time()

    print('Time is:%s'%(time2-time1)+'s')