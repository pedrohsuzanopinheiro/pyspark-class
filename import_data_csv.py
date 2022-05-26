import re

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("mytest").setMaster("local[*]")
sc = SparkContext(conf=conf)

file_rdd = sc.textFile("data/netflix_titles.csv")

# print("\n", file_rdd)
# print("\n", type(file_rdd))
# print("\n", dir(file_rdd))

COMMA_DELIMITER = re.compile(""",(?=(?:[^"]*"[^"]*")*[^"]*$)""")

# print("\n file has", file_rdd.count(), "rows")
# print("\n first line", file_rdd.first())

rows = file_rdd.map(lambda line: COMMA_DELIMITER.split(line))

listofrows = rows.map(lambda row: row).collect()
# print("\n", listofrows)

for row in listofrows:
    print(row, "\n")
