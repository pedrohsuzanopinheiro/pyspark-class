from colorama import Back, Fore, Style, init

init(autoreset=True)

from pyspark.sql import SparkSession

my_spark = (
    SparkSession.builder.appName("twitter")
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2",
    )
    .master("local[*]")
    .getOrCreate()
)

COLLECTION = "the_moyc"

df_followers = (
    my_spark.read.format("com.mongodb.spark.sql.DefaultSource")
    .option("uri", f"mongodb://127.0.0.1/twitter_followers.{COLLECTION}")
    .load()
)
df_friends = (
    my_spark.read.format("com.mongodb.spark.sql.DefaultSource")
    .option("uri", f"mongodb://127.0.0.1/twitter_friends.{COLLECTION}")
    .load()
)

print(Fore.WHITE + Back.MAGENTA + f"create sql temp table views")
df_followers.createOrReplaceTempView("twitter_followers")
df_friends.createOrReplaceTempView("twitter_friends")
print(my_spark.catalog.listTables())

print(Fore.WHITE + Back.MAGENTA + f"sql join")
query = """
SELECT
    tf.id_str,
    tf.created_at,
    tf.screen_name,
    tf.followers_count,
    tf.friends_count,
    tfr.screen_name
FROM twitter_followers tf 
LEFT JOIN twitter_friends tfr
ON tf.screen_name = tfr.screen_name
ORDER BY tf.followers_count DESC
"""

df = my_spark.sql(query)
print(df.show(100, truncate=False))
print(df.count())

my_spark.stop()
