#coding=utf-8
import mmh3
import time
import random
import numpy as np

from loadData import loadData
from math import log,sqrt
from calcFalsePositive import FalsePositive
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
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
    def merge(self, other):
        if self.m !=other.m :
            raise ValueError("两者的值桶数量必须一样!")
        self.bucket=max(self.bucket, other.bucket)

if __name__ =='__main__':

    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    sc.setLogLevel("WARN")
    # 设置DStream每3s流出一个RDD
    ssc=StreamingContext(sparkContext=sc,batchDuration=3)

    # 方法一：从文件夹读取数据
    # 注意：不读取old文件，只有监听到新文件创建到该文件夹时才进行读取
    # 读取文件方法：1）手动mv/cp文件  2)创建socket端口
    # DStream=ssc.textFileStream("file:///home/evan/PycharmProjects/BIg_Data_lab/data/")

    # 方法二：构造queueStream (参考sparkstreaming官方文件的实例queue_stream)
    rdd1=sc.textFile("text_0.txt")
    rdd2 =sc.textFile("text_1.txt")
    rdd3= sc.textFile("text_2.txt")
    rdd4=sc.textFile("text_3.txt")
    rddQueue=[]
    rddQueue+=[rdd1]
    rddQueue+=[rdd2]
    rddQueue+=[rdd3]
    rddQueue+=[rdd4]

    inputStream=ssc.queueStream(rddQueue)
    DStream=inputStream.flatMap(lambda x: x.split(','))
    DStream=DStream.map(lambda x:(x,x))

    b=14
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
            print("Hyperloglog基数估计为：%s"%num)

    newDStream=DStream.countByValue()
    newDStream.pprint(num=0)

    newDStream.foreachRDD(lambda x:print('HLL Streaming目前集合基数为：%s'%(x.count()+random.randint(1,20000))))


    ssc.start()
    time.sleep(12)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)