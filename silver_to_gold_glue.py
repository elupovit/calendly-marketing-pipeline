import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# ---- Delta-ready SparkSession (critical) ----
spark = (
    SparkSession.builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)

# GlueContext must be built from sparkContext (prevents _glue_scala_context error)
glue_context = GlueContext(spark.sparkContext)
job = Job(glue_context)

# ---- Args ----
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job.init(args["JOB_NAME"], args)

# ---- Paths ----
bucket = "calendly-marketing-pipeline-data"  # change if needed
silver_path = f"s3://{bucket}/silver_delta/calendly/events"
gold_path   = f"s3://{bucket}/gold/calendly/events"

try:
    print("🔧 Reading Silver Delta…", silver_path)
    silver = spark.read.format("delta").load(silver_path)

    # Basic sanity columns (adapt to your schema if needed)
    if "event_type" not in silver.columns:
        from pyspark.sql.functions import lit
        silver = silver.withColumn("event_type", lit("unknown"))

    if "event_duration" in silver.columns:
        from pyspark.sql.functions import col
        duration_col = col("event_duration").cast("double")
    else:
        from pyspark.sql.functions import lit
        duration_col = lit(None).cast("double")

    print("📊 Aggregating to Gold…")
    gold = (
        silver.groupBy(col("event_type"))
              .agg(
                  count("*").alias("total_events"),
                  avg(duration_col).alias("avg_duration_seconds")
              )
    )

    print("💾 Writing Gold Delta…", gold_path)
    (gold.write
         .format("delta")
         .mode("overwrite")
         .save(gold_path))

    print("✅ Gold write complete.")
    job.commit()

except Exception as e:
    print(f"❌ Job failed: {e}")
    # Write a small clue into CloudWatch by raising after printing
    raise
