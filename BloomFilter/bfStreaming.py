# coding: utf-8
import math
import numpy as np
import BitVector
import random
import time

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from calcFalsePositive import FalsePositive

# Hash Table法：最简单最快的Hash，但准确度较差(分布不均匀)
class SimpleHash():
    def __init__(self, M, seed):
        self.m = M
        self.seed = seed

    def hash(self, value):
        ret = 0
        # for i in range(len(value)):
        #     ret+=self.seed*ret+value[i]
        ret = self.seed * value % self.m
        return ret


class BloomFilter():
    def __init__(self, m, k):
        if not (m > 0):
            raise ValueError("位向量长度必须为正")
        if not (k > 0):
            raise ValueError("哈希函数个数必须为正")

        self.m = m  # 位向量长度
        self.k = k  # 哈希函数个数
        self.seeds = [283,491,677,991,1331,1971,3737,4717,5717,7157]  # 哈希种子。一系列素数
        self.BIT_SIZE = m
        self.bitset = BitVector.BitVector(size=self.BIT_SIZE)
        self.hashFunc = []
        self.false_positive = 0
        for i in range(self.k):
            self.hashFunc.append(SimpleHash(self.BIT_SIZE, self.seeds[i]))

    # 将元素插入位向量
    def insert(self, value):
        for f in self.hashFunc:
            loc = f.hash(value)
            self.bitset[loc] = 1

    # 判断新元素是否在集合中已经存在,返回True/False
    def contains(self, value):
        if value == None:
            print("没有元素！")
            return False

        ret = True
        for f in self.hashFunc:
            loc = f.hash(value)
            ret = ret & self.bitset[loc]  # 只要交为0则一直为0
            # print(ret)
        return ret


if __name__ == '__main__':
    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    sc.setLogLevel("WARN")
    # 设置DStream每4s流出一个RDD
    ssc=StreamingContext(sparkContext=sc,batchDuration=4)

    # 方法一：从文件夹读取数据
    # 注意：不读取old文件，只有监听到新文件创建到该文件夹时才进行读取
    # 读取文件方法：1）手动mv/cp文件  2)创建socket端口
    # DStream=ssc.textFileStream("file:///home/evan/PycharmProjects/BIg_Data_lab/data/")
    # 方法二：从HDFS读取，同样地只能读取新监听到的文件
    # DStream=ssc.textFile("hdfs://0.0.0.0:9000/user/evan/input/data_4.txt")
    # 方法三：构造queueStream (参考sparkstreaming官方文件的实例queue_stream)
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
    DStream=inputStream.flatMap(lambda x: x.split(',')).map(lambda x: int(x))
    newDS=DStream.map(lambda x: exist(x) is True)
    DStream.pprint(num=0)
    newDS.foreachRDD(lambda x: print('目前为止不同元素为：%s'%x.count()))

    # 保存输出结果为txt到文件中，按时间顺序排列
    # DStream.saveAsTextFiles("file:///home/evan/PycharmProjects/BIg_Data_lab/data/result.txt")

    m = 10000000  # 位向量长度
    # n = rdd2.count()  # 数据总容量
    # k=math.ceil((m/n)* math.log(2)) #根据求导得出的最优hash个数
    # 构造BloomFilter
    k = 10
    bf = BloomFilter(m, k)

    def exist(x):
        flag = False
        if not (bf.contains(x)):
            bf.insert(x)
            flag = True
        return flag

    ssc.start()
    time.sleep(20)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
    # ssc.awaitTermination()


