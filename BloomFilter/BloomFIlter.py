# coding: utf-8
import math
import numpy as np
import BitVector
import random
import time

from pyspark import SparkContext
from pyspark import SparkConf
from loadData import loadData
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
        return ret


if __name__ == '__main__':
    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    sc.setLogLevel("WARN")
    rdd1 = sc.textFile("file:///home/evan/PycharmProjects/BIg_Data_lab/text_3.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(',')).map(lambda x: int(x))

    time1 = time.time()
    m = 100000000 #位向量长度
    n = rdd2.count()  #数据总容量
    # k=math.ceil((m/n)* math.log(2)) #根据求导得出的最优hash个数
    # 构造BloomFilter
    k=10
    bf = BloomFilter(m, k)

    # 取出RDD中某行到某行的元素
    # stand = RDD.zipWithIndex().filter(lambda x: 0 <= x[1] < 100000).map(lambda x: x[0])

    # 判断x是否在bf中，若不在则加入bf中
    def exist(x):
        flag = False
        if not (bf.contains(x)):
            bf.insert(x)
            flag = True
        return flag

    newRDD = rdd2.filter(lambda x: exist(x) is True)
    time2=time.time()
    bfnum=newRDD.count()
    allnum,fp=FalsePositive(n, bfnum)
    print("BloomFilter方法估计集合基数为：%s"%bfnum)
    print("多重集合实际基数为：%s"%allnum)
    print("误差率Error_rate为：%s"%(fp*100)+"%")
    print("time is:%s"%(time2-time1)+'s')
    # time.sleep(180)