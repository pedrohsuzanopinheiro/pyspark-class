from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("mytest").setMaster("local[*]")
sc = SparkContext(conf=conf)

print(type(sc), "\n")
print(dir(sc), "\n")
print(sc.version, "\n")
