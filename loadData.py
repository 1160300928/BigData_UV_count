from pyspark import SparkContext, SparkConf

def loadData():
    # 创建sc对象,local[4]表示：本地4个核，CardinalityEstimate为APPname
    conf = (SparkConf().set('spark.driver.memory', '6656m').set('spark.executor.memory','5g'))
    sc = SparkContext("local[4]", "CardinalityEstimate", conf=conf)
    # 设置日志级别
    sc.setLogLevel("WARN")
    # 从hdfs读取数据
    # rdd=sc.textFile("hdfs://0.0.0.0:9000/user/evan/input/data_4.txt")
    # 从本地读取数据
    rdd1=sc.textFile("data_3.txt")
    rdd2=rdd1.flatMap(lambda x: x.split(','))
    numRDD=rdd2.map(lambda x: int(x))
    print('RDD中所有元素个数为：%s'%(numRDD.count()))
    return numRDD

