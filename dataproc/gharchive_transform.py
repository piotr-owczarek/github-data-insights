import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour, when, col, to_date

parser = argparse.ArgumentParser()
parser.add_argument('--input_path', required=True)
parser.add_argument('--fact_user_activity', required=True)
parser.add_argument('--fact_repo_popularity', required=True)
parser.add_argument('--fact_hourly_activity', required=True)
args = parser.parse_args()

input_path = args.input_path
fact_user_activity = args.fact_user_activity
fact_repo_popularity = args.fact_repo_popularity
fact_hourly_activity = args.fact_hourly_activity

spark = SparkSession.builder \
    .appName('gharchive_app') \
    .getOrCreate()

#temp bucket created by Dataproc - change required if creating a new cluster
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west1-846427390107-lngoajud')

df_raw = spark.read.parquet(input_path)

df_raw = df_raw.withColumn("created_at", to_timestamp("created_at")) \
               .withColumn("date", to_date("created_at")) \
               .withColumn("hour", hour("created_at")) \
               .drop("created_at")

df_clean = df_raw.select(
    col("id"),
    col("type"),
    col("user"),
    col("date"),
    col("hour"),
    when(col("user").rlike("(?i)(bot|\\[bot\\]|_bot)$"), "bot").otherwise("human").alias("user_type")
)

df_user_activity = df_clean.groupBy("date", "hour", "user_type") \
    .count() \
    .withColumnRenamed("count", "event_count")

df_repo_popularity = df_raw.filter(
    col("organization").isNotNull()
).select(
    col("organization"),
    col("repository"),
    col("date")
).groupBy("date", "organization", "repository") \
 .count() \
 .withColumnRenamed("count", "event_count")

df_hourly_activity = df_raw.groupBy("date", "hour") \
    .count() \
    .withColumnRenamed("count", "event_count")

#Append to BigQuery tables
df_user_activity.write.format("bigquery") \
    .mode("append") \
    .option("table", fact_user_activity) \
    .save()

df_repo_popularity.write.format("bigquery") \
    .mode("append") \
    .option("table", fact_repo_popularity) \
    .save()

df_hourly_activity.write.format("bigquery") \
    .mode("append") \
    .option("table", fact_hourly_activity) \
    .save()