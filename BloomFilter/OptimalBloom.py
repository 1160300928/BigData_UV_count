# coding: utf-8
import math
import time
import BitVector

from loadData import loadData
from Hash.Jenkins_hash import jhash_short
from pyspark import SparkConf,SparkContext
from calcFalsePositive import FalsePositive

class JenkinsHash():

  def __init__(self,M):
    self.m = M

  def base_hash_function(self,key):
    return jhash_short(key)

  def convert(self,y):
    return  self.base_hash_function(y)  % self.m


class PrimalBloom():
    def __init__(self, m, k):
        if not (m > 0):
            raise ValueError("位向量长度必须为正")
        if not (k > 0):
            raise ValueError("哈希函数个数必须为正")

        self.m = m  # 位向量长度
        self.k = k  # 哈希函数个数
        self.BIT_SIZE = m
        self.bitset = BitVector.BitVector(size=self.BIT_SIZE)
        self.hashFunc = []
        self.false_positive = 0 #FP
        for i in range(self.k):
            obj=JenkinsHash(self.m)
            self.hashFunc.append(obj)

    # 将元素插入位向量
    def insert(self, value):
        for f in self.hashFunc:
            hash= f.convert(value)
            loc = int(hash)
            self.bitset[loc] = 1

    # 判断新元素是否在集合中已经存在,返回True/False
    def contains(self, value):
        if value == None:
            print("没有元素！")
            return False
        ret = True
        for f in self.hashFunc:
            loc = int(f.convert(value))
            ret = ret & self.bitset[loc]
            # print(ret)
        return ret

if __name__ == '__main__':
    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    sc.setLogLevel("WARN")
    rdd1 = sc.textFile("file:///home/evan/PycharmProjects/BIg_Data_lab/text_3.txt")
    RDD = rdd1.flatMap(lambda x: x.split(','))

    time1=time.time()
    m = 100000000 #位向量长度
    n = RDD.count()  #数据总容量
    k=math.ceil((m/n)* math.log(2)) #根据求导得出的最优hash个数
    # 构造BloomFilter
    pb = PrimalBloom(m, k)
    strRDD = RDD
    # 取出RDD中某行到某行的元素
    # stand = RDD.zipWithIndex().filter(lambda x: 0 <= x[1] < 100000).map(lambda x: x[0])
    # 判断x是否在bf中，若不在则加入bf中
    def exist(x):
        flag = False
        if not (pb.contains(x)):
            pb.insert(x)
            flag = True
        return flag
    newRDD = strRDD.filter(lambda x: exist(x) is True)
    time2=time.time()

    bfnum = newRDD.count()
    allnum, fp = FalsePositive(n, bfnum)
    print("OptimalBloom方法估计集合基数为：%s" % bfnum)
    print("多重集合实际基数为：%s" % allnum)
    print("FalsePositive占比为：%s" % (fp * 10) + "%")
    print("time is :%s"%(time2-time1)+'s')