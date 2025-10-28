from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
import sys

# ---------------------------
# CONFIG
# ---------------------------
args = {a.split('=')[0]: a.split('=')[1] for a in sys.argv[1:] if '=' in a}
bronze_calendly_path = args.get("bronze_calendly_path", "s3://calendly-marketing-pipeline-data/bronze/calendly/webhooks/*")
bronze_spend_path    = args.get("bronze_spend_path",    "s3://calendly-marketing-pipeline-data/bronze/marketing_spend/*")
silver_events_path   = args.get("silver_events_path",   "s3://calendly-marketing-pipeline-data/silver/calendly/events/")
silver_spend_path    = args.get("silver_spend_path",    "s3://calendly-marketing-pipeline-data/silver/marketing_spend/")
report_timezone = "America/New_York"
DELTA_PACKAGE = "io.delta:delta-core_2.12:2.3.0"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze-to-silver")

# ---------------------------
# SPARK SESSION
# ---------------------------
spark = (
    SparkSession.builder.appName("BronzeToSilverCalendlyAndSpend")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.jars.packages", DELTA_PACKAGE)
    .getOrCreate()
)

# =========================================================
# PART 1: CALENDLY EVENTS PROCESSING
# =========================================================
logger.info("Processing Calendly event data...")

df_raw = spark.read.option("multiline", "true").json(bronze_calendly_path)
logger.info(f"Read raw Calendly records: {df_raw.count()}")

df = df_raw.select(
    F.col("created_at").alias("webhook_created_at"),
    F.col("created_by").alias("webhook_created_by"),
    F.col("event").alias("webhook_event"),
    F.col("payload.name").alias("invitee_name"),
    F.col("payload.email").alias("invitee_email"),
    F.col("payload.timezone").alias("invitee_timezone"),
    F.col("payload.uri").alias("invitee_uri"),
    F.col("payload.cancel_url").alias("cancel_url"),
    F.col("payload.reschedule_url").alias("reschedule_url"),
    F.col("payload.status").alias("invitee_status"),
    F.col("payload.scheduling_method").alias("scheduling_method"),
    F.col("payload.rescheduled").alias("is_rescheduled"),
    F.col("payload.created_at").alias("invitee_created_at"),
    F.col("payload.updated_at").alias("invitee_updated_at"),
    F.to_json(F.col("payload.questions_and_answers")).alias("questions_and_answers_json"),
    F.col("payload.scheduled_event.uri").alias("scheduled_event_uri"),
    F.col("payload.scheduled_event.name").alias("scheduled_event_name"),
    F.col("payload.scheduled_event.event_type").alias("event_type_uri"),
    F.col("payload.scheduled_event.start_time").alias("event_start_time_utc"),
    F.col("payload.scheduled_event.end_time").alias("event_end_time_utc"),
    F.col("payload.scheduled_event.status").alias("event_status"),
    F.expr("payload.scheduled_event.event_memberships[0].user").alias("host_user_uri"),
    F.expr("payload.scheduled_event.event_memberships[0].user_email").alias("host_user_email"),
    F.expr("payload.scheduled_event.event_memberships[0].user_name").alias("host_user_name"),
    F.col("payload.tracking.utm_source").alias("utm_source"),
    F.col("payload.tracking.utm_medium").alias("utm_medium"),
    F.col("payload.tracking.utm_campaign").alias("utm_campaign"),
    F.col("payload.tracking.utm_term").alias("utm_term"),
    F.col("payload.tracking.utm_content").alias("utm_content")
)

# --- timestamps & derived columns
df = (
    df
    .withColumn("event_start_ts", F.to_timestamp("event_start_time_utc"))
    .withColumn("event_end_ts", F.to_timestamp("event_end_time_utc"))
    .withColumn("event_start_local", F.from_utc_timestamp("event_start_ts", report_timezone))
    .withColumn("event_end_local", F.from_utc_timestamp("event_end_ts", report_timezone))
    .withColumn("booking_date", F.to_date("event_start_local"))
    .withColumn("booking_hour", F.hour("event_start_local"))
    .withColumn("day_of_week", F.date_format("event_start_local", "EEEE"))
)

# --- extract event_type_id from URI
df = df.withColumn("event_type_id", F.regexp_extract("event_type_uri", r'([^/]+)$', 1))

# --- mapping expression
mapping_expr = F.create_map(
    F.lit("d639ecd3-8718-4068-955a-436b10d72c78"), F.lit("facebook"),
    F.lit("dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098"), F.lit("youtube"),
    F.lit("bb339e98-7a67-4af2-b584-8dbf95564312"), F.lit("tiktok")
)

# --- FIXED channel mapping (mapping first, then utm_source)
df = df.withColumn(
    "channel",
    F.coalesce(
        mapping_expr[F.lower(F.col("event_type_id"))],
        F.lower(F.col("utm_source")),
        F.lit("unknown")
    )
)

# --- Deduplicate
dedupe_key = F.coalesce(F.col("invitee_uri"), F.concat_ws("_", F.col("invitee_email"), F.col("event_start_time_utc")))
window = Window.partitionBy(dedupe_key, F.col("scheduled_event_uri")).orderBy(F.desc("invitee_created_at"))
df_dedup = df.withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")

# --- Add partition column
out_df = df_dedup.withColumn("dt", F.current_date())

# --- Diagnostic summary
logger.info("Channel distribution before write:")
channel_counts = out_df.groupBy("channel").count().orderBy(F.desc("count"))
for row in channel_counts.collect():
    logger.info(f"{row['channel']}: {row['count']}")

# --- Write to Silver
try:
    out_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("dt") \
        .save(silver_events_path)
    logger.info(f"✅ Wrote flattened silver Delta data to {silver_events_path}")
except Exception as e:
    logger.error(f"Delta write failed, fallback to Parquet: {e}")
    out_df.write.mode("overwrite").partitionBy("dt").parquet(silver_events_path)

# =========================================================
# PART 2: MARKETING SPEND PROCESSING
# =========================================================
logger.info("Processing marketing spend data...")

df_spend_raw = spark.read.option("multiline", "true").json(bronze_spend_path)
logger.info(f"Read raw spend records: {df_spend_raw.count()}")

df_spend = (
    df_spend_raw
    .select("date", "channel", "spend")
    .withColumn("dt", F.to_date("date"))
    .dropDuplicates(["date", "channel"])
)

try:
    df_spend.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("dt") \
        .save(silver_spend_path)
    logger.info(f"✅ Appended spend data to unified Delta table at {silver_spend_path}")
except Exception as e:
    logger.error(f"Delta write failed for spend data, fallback to Parquet append: {e}")
    df_spend.write.mode("append").partitionBy("dt").parquet(silver_spend_path)

spark.stop()
