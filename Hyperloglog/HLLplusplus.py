import mmh3
import time


from loadData import loadData
from math import log,sqrt
from calcFalsePositive import FalsePositive
from pyspark import SparkContext,SparkConf
# from Hyperloglog.HLL64Constants import biasData, treshold

# hllpp:
# 采用64bit hash，进一步减小碰撞率
# 将参数b增大至18，最多可分2**18个bucket 4-18
# 增加bias偏差修正

class HLLplusplus():

    def __init__(self,b):
        if not (2 <= b <= 18): #hll++把b的值增大到了18
            raise ValueError("输入的b必须在2到18之间!")
        self.b=b
        self.m=2**b
        self.alphaMM=self.alphaMM(self.m)
        #self.bucket=BitVector.BitVector(size=self.m)
        self.bucket=[0]*self.m
        self.error=1.04/sqrt(self.m)
        # self.bias=biasData
        # self.threshold=treshold

    """
    #偏差修正
    def bias(self,E,m):
        V=float(self.bucket.count(0))
        if E<=5*m:
            # Estimate=E-biasData[m*E-1]
        else:
            Estimate=E
        if not V==0:
            H =self.m * log(self.m/ V)
        if V==0:
            H = Estimate
        if H<=self.threshold[m]:
            return H
        else:
            return Estimate
    """

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
        x=mmh3.hash64(strvalue,signed=False)
        a=64-self.b
        i=x>>(64-self.b) #取64bit哈希值的前b位
        v=self.left_most_nbit(x<<self.b, a) #除去前b位剩下的前导0
        self.bucket[i]=max(self.bucket[i], v)

    #获取最左边的n位
    def left_most_nbit(self,x,m):
        v=1
        while v<=m and not x&0x8000000000000000:
            v=v+1
            x=x<<1
        return v

    #计算m个桶内估算结果的调和平均值,然后推导总体估计值E
    #修正偏差，分情况讨论
    def count(self):
        estimate=self.alphaMM / sum([2**(-v) for v in self.bucket])
        max=1<<64
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
            raise ValueError("两者的精度必须相同!")
        self.bucket=max(self.bucket, other.bucket)


if __name__ =='__main__':
    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory', '7g'))
    sc = SparkContext("local[4]", "CardinalityEstimate",conf=conf)
    sc.setLogLevel("WARN")
    rdd1 = sc.textFile("file:///home/evan/PycharmProjects/BIg_Data_lab/text_3.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(','))
    strRDD=rdd2

    time1 = time.time()
    b=17
    n=strRDD.count()
    hll=HLLplusplus(b)


    def add(value):
        x = mmh3.hash64(value[0], signed=False)[1]
        a = 64 - hll.b
        i = x >> (64 - hll.b)  # 取64bit哈希值的前b位
        v = hll.left_most_nbit(x << hll.b, a)  # 除去前b位剩下的前导0
        hll.bucket[i] = max(hll.bucket[i], v)
        if value[1]==n-1:
            num=hll.count()
            all,fp=FalsePositive(n,num)
            print()
            print("HLL++基数估计为：%s"%num)
            print("FP百分率为：%s"%(fp*100)+'%')


    RDD=strRDD.zipWithIndex().map(lambda x: add(x))
    time2=time.time()

    RDD.count()
    print("----time is:%s"%(round(time2-time1,6))+'s')


