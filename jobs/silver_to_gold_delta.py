from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, unix_timestamp

# 1. Initialize SparkSession with Delta support
spark = (
    SparkSession.builder.appName("silver_to_gold_delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# 2. Define input (Silver Delta) and output (Gold Delta) paths
silver_path = "s3://calendly-marketing-pipeline-data/silver_delta/calendly/events/"
gold_path   = "s3://calendly-marketing-pipeline-data/gold/calendly/events/"

# 3. Read Silver Delta table
silver_df = spark.read.format("delta").load(silver_path)

# 4. Compute event_duration dynamically (seconds)
silver_df = silver_df.withColumn(
    "event_duration",
    unix_timestamp("event_end_time") - unix_timestamp("event_start_time")
)

# 5. Aggregate results by event_type
gold_df = (
    silver_df.groupBy("event_type")
    .agg(
        count("*").alias("total_events"),
        avg(col("event_duration")).alias("avg_duration_seconds")
    )
)

# 6. Write results as Gold Delta table
gold_df.write.format("delta").mode("overwrite").save(gold_path)

print("✅ Successfully wrote Gold Delta table.")
spark.stop()
