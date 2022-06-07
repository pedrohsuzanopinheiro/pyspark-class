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

dffollowers = df_followers.select(
    "id_str",
    "created_at",
    "screen_name",
    "followers_count",
    "friends_count",
)
dffriends = df_friends.select(
    "id_str",
    "created_at",
    "screen_name",
    "followers_count",
    "friends_count",
)

print(Fore.WHITE + Back.MAGENTA + f"df_followers {type(df_followers)}")
print(dffollowers.show())

print(Fore.WHITE + Back.MAGENTA + f"df_friends {type(df_friends)}")
print(dffriends.show())

my_spark.stop()
