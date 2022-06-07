from colorama import Back, Fore, Style, init
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

init(autoreset=True)

input_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"
output_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"

my_spark = (
    SparkSession.builder.appName("twitter").master("local[*]").getOrCreate()
)

file = "data/twitter_followers/the_moyc.json"
df_followers = my_spark.read.json(file)

file = "data/twitter_friends/the_moyc.json"
df_friends = my_spark.read.json(file)

file = "data/netflix_titles.csv"
df_netflix = (
    my_spark.read.format("csv")
    .option("inferschema", True)
    .option("header", True)
    .option("sep", ",")
    .load(file)
)

dffollowers = df_followers.select(
    "id_str",
    "created_at",
    "created_at_date",
    "screen_name",
    "followers_count",
    "friends_count",
)
dffriends = df_friends.select(
    "id_str",
    "created_at",
    "created_at_date",
    "screen_name",
    "followers_count",
    "friends_count",
)
dfnetflix = df_netflix.select("show_id", "title", "cast", "release_year")

print(Fore.WHITE + Back.MAGENTA + f"df_followers {type(df_followers)}")
print(dffollowers.show())

print(Fore.WHITE + Back.MAGENTA + f"df_friends {type(df_friends)}")
print(dffriends.show())

print(Fore.WHITE + Back.MAGENTA + f"df_netflix {type(df_netflix)}")
print(dfnetflix.show())

my_spark.stop()
