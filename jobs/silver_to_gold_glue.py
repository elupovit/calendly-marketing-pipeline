import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, count, avg

# Initialize Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

print("✅ Glue job started: silver → gold")

# Paths
silver_path = "s3://calendly-marketing-pipeline-data/silver_delta/calendly/events/"
gold_path = "s3://calendly-marketing-pipeline-data/gold/calendly/events/"

try:
    # ==========================
    # LOAD SILVER DELTA
    # ==========================
    print(f"📥 Reading Silver Delta from {silver_path}")
    silver = spark.read.format("delta").load(silver_path)

    # ==========================
    # DATA CLEANUP
    # ==========================
    if "created_at" in silver.columns:
        silver = silver.withColumn("event_date", to_date(col("created_at")))
    else:
        silver = silver.withColumn("event_date", to_date(col("event_start_time")))

    if "event_type" not in silver.columns:
        silver = silver.withColumn("event_type", col("organizer_email"))

    print("✅ Silver schema:")
    silver.printSchema()

    # ==========================
    # AGGREGATE TO GOLD
    # ==========================
    print("⚙️ Aggregating Silver → Gold ...")
    gold = (
        silver.groupBy("event_type", "event_date")
        .agg(
            count("*").alias("total_events"),
            avg("event_duration").alias("avg_duration_seconds")
        )
    )

    # ==========================
    # WRITE GOLD DELTA
    # ==========================
    print(f"💾 Writing Gold Delta to {gold_path}")
    gold.write.format("delta").mode("overwrite").save(gold_path)
    print("✅ Gold Delta written successfully!")

except Exception as e:
    print(f"❌ Glue job failed with error: {e}")
    job.commit()
    sys.exit(1)

# ==========================
# CLEAN EXIT
# ==========================
print("🎉 Job completed successfully.")
job.commit()
