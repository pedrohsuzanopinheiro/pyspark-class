from colorama import Back, Fore, Style, init
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

init(autoreset=True)

input_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"
output_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"

my_spark = SparkSession.builder.appName("twitter").master("local[*]").getOrCreate()

file = "data/twitter_followers/the_moyc.json"
df_followers = my_spark.read.json(file)

file = "data/twitter_friends/the_moyc.json"
df_friends = my_spark.read.json(file)

print(Fore.WHITE + Back.MAGENTA + f"df_followers {type(df_followers)}")
print(Fore.WHITE + Back.MAGENTA + f"df_friends {type(df_friends)}")
