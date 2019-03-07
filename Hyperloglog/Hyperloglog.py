#coding=utf-8
import mmh3
import time
import numpy as np

from loadData import loadData
from math import log,sqrt
from calcFalsePositive import FalsePositive
from pyspark import SparkContext,SparkConf
from pyspark.accumulators import AccumulatorParam

class HyperLoglog():

    def __init__(self,b):
        if not (2 <= b <= 16):
            raise ValueError("输入的b必须在2到16之间!")
        self.b=b
        self.m=2**b
        self.alphaMM=self.alphaMM(self.m)
        self.bucket=[0]*self.m
        self.error=1.04/sqrt(self.m)

    #修正参数alpha。此处计算值为alpha*m*m，为整体参数
    def alphaMM(self,m):
        if self.m==16:
            return 0.673*m*m
        if self.m==32:
            return 0.697*m*m
        if self.m==64:
            return 0.709*m*m
        return 0.7213/(1+ 1.079/self.m)*m*m

    #向bucket中添加元素
    def add(self,strvalue):
        x=mmh3.hash(strvalue,signed=False)
        a=32-self.b
        i=x>>(32-self.b) #取32bit哈希值的前b位
        v=self.left_most_nbit(x<<self.b, a) #除去前b位剩下的前导0
        self.bucket[i]=max(self.bucket[i], v)

    #获取最左边的n位
    def left_most_nbit(self,x,m):
        v=1
        while v<=m and not x&0x80000000:
            v=v+1
            x=x<<1
        return v

    #计算m个桶内估算结果的调和平均值,然后推导总体估计值E
    #修正偏差，分情况讨论
    def count(self):
        estimate=self.alphaMM / sum([2**(-v) for v in self.bucket])
        max=1<<32
        if estimate<= 2.5* self.m:
            V=float(self.bucket.count(0))
            return round(self.m * log(self.m/ V))
        if estimate>(max / 30):
            return round(-max*log(1-estimate/max))
        else:
            return round(estimate)

    #合并几个同类对象的结果
    def merge(self, otherRDD):
        if self.m !=otherRDD.m :
            raise ValueError("两者的值桶数量必须一样!")
        self.bucket=max(self.bucket, otherRDD.bucket)

if __name__ =='__main__':
    # accumulator：
    # accum = sc.accumulator(0)

    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    sc.setLogLevel("WARN")
    rdd1 = sc.textFile("file:///home/evan/PycharmProjects/BIg_Data_lab/text_3.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(','))

    time1=time.time()
    strRDD=rdd2
    b=16
    n=strRDD.count()
    hll=HyperLoglog(b)

    def add(value):
        x = mmh3.hash(value[0], signed=False)
        a = 32 - hll.b
        i = x >> (32 - hll.b)  # 取32bit哈希值的前b位
        v = hll.left_most_nbit(x << hll.b, a)  # 除去前b位剩下的前导0
        hll.bucket[i] = max(hll.bucket[i], v)
        # print(type(hll.bucket))
        if value[1]==n-1:
            num=hll.count()
            all,fp=FalsePositive(n,num)
            print()
            print("Hyperloglog基数估计为：%s"%num)
            print("FP百分率为：%s"%(fp*100)+'%')

    #之前一直想把RDD中的外部变量传出来，但由于RDD是闭包的，则直接在RDD内保存记录结果即可
    RDD=strRDD.zipWithIndex().map(lambda x: add(x))
    time2 = time.time()
    RDD.count()

    print("----Time is:%s"%(round(time2-time1,6))+'s----')
    # time.sleep(180)