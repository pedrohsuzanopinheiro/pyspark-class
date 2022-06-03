import datetime

from colorama import Back, Fore, Style, init
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

init(autoreset=True)

input_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"
output_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"

myspark = (
    SparkSession.builder.appName("twitter")
    .config("spark.mongodb.input.uri", input_uri)
    .config("spark.mongodb.output.uri", output_uri)
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2",
    )
    .getOrCreate()
)

df = myspark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df1 = (
    df.withColumn(
        "created_date_cleaned",
        f.regexp_replace(f.col("created_at"), "^[A-Za-z]+", ""),
    )
    .withColumn(
        "created_timestamp",
        f.to_timestamp(f.col("created_date_cleaned"), " MMM dd HH:mm:ss Z yyyy"),
    )
    .withColumn(
        "created_date",
        f.to_date("created_timestamp", "MMM dd HH:mm:ss Z yyyy"),
    )
)

# create view
view_name = "twitter_user_timeline_guardian"
df1.createOrReplaceTempView(view_name)

# query
SQL_QUERY = f"SELECT id, created_at, name, location FROM {view_name} WHERE created_date BETWEEN '2020-05-01' AND '2020-05-31'"
df2 = myspark.sql(SQL_QUERY)

for row in df2.rdd.collect():
    print(row.created_at, row.name)
