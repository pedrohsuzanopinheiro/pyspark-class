from pyspark.sql import SparkSession

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

# print(df.printSchema())
print(df.first())
