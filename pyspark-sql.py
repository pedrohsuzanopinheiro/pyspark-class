import datetime

from colorama import Back, Fore, Style, init
from pyspark.sql import SparkSession

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

date_start, date_end = (
    str(datetime.datetime.strptime("2020-05-01", "%Y-%m-%d").isoformat()) + "Z",
    str(datetime.datetime.strptime("2020-05-31", "%Y-%m-%d").isoformat()) + "Z",
)
print(
    Fore.WHITE + Back.GREEN + f"date_start: {date_start}",
    Fore.WHITE + Back.CYAN + f"date_end: {date_end}",
)

# gt greater than
# lt last than
pipeline = {
    "$match": {
        "$and": [
            {"created_at_date": {"$gt": {"$date": date_start}}},
            {"created_at_date": {"$lt": {"$date": date_end}}},
        ],
        # "text": {"$regex": "covid", "$options": "i"},
    }
}

df = (
    myspark.read.format("com.mongodb.spark.sql.DefaultSource")
    .option("pipeline", pipeline)
    .load()
)

# create view
view_name = "twitter_user_timeline_guardian"
df.createOrReplaceTempView(view_name)

# query
SQL_QUERY = f"SELECT id, created_at, name, location FROM {view_name}"
new_df = myspark.sql(SQL_QUERY)

print(new_df.show(new_df.count()))
