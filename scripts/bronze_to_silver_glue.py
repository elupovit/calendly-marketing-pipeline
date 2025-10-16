from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

bucket = "calendly-marketing-pipeline-data"
bronze_path = f"s3://{bucket}/bronze/calendly/events/"
silver_path = f"s3://{bucket}/silver_delta/calendly/events/"
database = "calendly_marketing"
table = "silver_calendly_events_delta"

spark = SparkSession.builder.appName("bronze_to_silver_glue").getOrCreate()

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

df = spark.read.format("json").option("multiLine", "false").load(bronze_path)

for c in ["event_start_time", "event_end_time", "created_at"]:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast("timestamp"))

if set(["event_start_time", "event_end_time"]).issubset(df.columns):
    df = df.withColumn(
        "event_duration",
        unix_timestamp(col("event_end_time")) - unix_timestamp(col("event_start_time"))
    )

df.write.format("delta").mode("overwrite").save(silver_path)

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {database}.{table}
  USING DELTA
  LOCATION '{silver_path}'
""")

print(f"✅ Silver Delta ready: {database}.{table} at {silver_path}")
spark.stop()
