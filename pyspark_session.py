from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("mytest").getOrCreate()

print(type(sc), "\n")
print(dir(sc), "\n")
print(sc.version, "\n")
