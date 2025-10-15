# scripts/bronze_to_silver_calendly.py
# Bronze (Calendly webhooks JSON) -> Silver (normalized Parquet, partitioned by dt)
# Robust to: mixed JSON shapes, payload wrapper, single-object-per-file, multiline, missing fields.

import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ---------------- Params ----------------
args = {a.split('=')[0]: a.split('=')[1] for a in sys.argv[1:] if '=' in a}
bronze_path = args.get('bronze_path', 's3://calendly-marketing-pipeline-data/bronze/calendly/webhooks/')
silver_path = args.get('silver_path', 's3://calendly-marketing-pipeline-data/silver/calendly/events/')

# ---------------- Spark -----------------
spark = (
    SparkSession.builder
    .appName("bronze_to_silver_calendly")
    .getOrCreate()
)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ---------------- Helpers ----------------
def j(path: str):
    """Safe JSON extractor at absolute JSONPath (NULL if missing)."""
    return F.get_json_object(F.col("raw_str"), path)

def jj(*relpaths):
    """
    Extract from both possible roots:
      - $.<relpath>
      - $.payload.<relpath>
    Returns first non-null among all options.
    """
    exprs = []
    for p in relpaths:
        exprs.append(j(f"$.{p}"))
        exprs.append(j(f"$.payload.{p}"))
    return F.coalesce(*exprs)

def parse_ts(scol):
    """Handle ISO-8601 with/without millis and with Z or ±offset."""
    return F.coalesce(
        F.to_timestamp(scol, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        F.to_timestamp(scol, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(scol, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(scol, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        F.to_timestamp(scol)  # Spark generic fallback
    )

# ---------------- Read bronze as TEXT (super robust) ----------------
# Only read .json files; skip marker/keep files.
text_df = (
    spark.read
    .text(bronze_path + "**/*.json")
    .withColumnRenamed("value", "raw_str")
    .withColumn("source_path", F.input_file_name())
)

# Drop blank / tiny lines quickly
text_df = text_df.filter(F.col("raw_str").isNotNull() & (F.length("raw_str") > 2))

# ---------------- Extract & normalize ----------------
flat = (
    text_df
    # Identifiers / meta
    .withColumn("event_uuid",          jj("event.uuid", "scheduled_event.uuid", "event.id", "id"))
    .withColumn("event_type_name",     jj("event_type.name", "event.name", "event_type"))
    .withColumn("event_type_slug",     jj("event_type.slug", "event_type"))
    .withColumn("scheduled_event_uri", jj("scheduled_event.uri", "event.uri", "uri"))
    # Invitee
    .withColumn("invitee_name",        jj("invitee.name", "invitee_name"))
    .withColumn("invitee_email",       jj("invitee.email", "invitee_email"))
    # Times
    .withColumn("event_start_time",    parse_ts(F.coalesce(jj("scheduled_event.start_time","event.start_time","start_time","start"))))
    .withColumn("event_end_time",      parse_ts(F.coalesce(jj("scheduled_event.end_time","event.end_time","end_time","end"))))
    .withColumn("created_at",          parse_ts(F.coalesce(jj("created_at","triggered_at","event.created_at","invitee.created_at"))))
    # System meta
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

# Final event_type
flat = flat.withColumn("event_type", F.coalesce(F.col("event_type_slug"), F.col("event_type_name")))

# Keep full raw JSON (for audit/lineage)
flat = flat.withColumn("raw_payload", F.col("raw_str"))

# Guaranteed partition column: prefer start_time, else created_at, else ingestion day
flat = flat.withColumn(
    "dt",
    F.date_format(F.coalesce(F.col("event_start_time"), F.col("created_at"), F.col("ingestion_timestamp")), "yyyy-MM-dd")
)

# Cast / tidy
flat = (
    flat
    .withColumn("event_uuid", F.col("event_uuid").cast("string"))
    .withColumn("invitee_name", F.col("invitee_name").cast("string"))
    .withColumn("invitee_email", F.trim(F.lower(F.col("invitee_email").cast("string"))))
    .drop("raw_str")  # we keep raw_payload only
)

# ---------------- Idempotent dedupe ----------------
dedupe_key = F.when(F.col("event_uuid").isNotNull(), F.col("event_uuid")) \
              .otherwise(F.sha2(F.coalesce(F.col("raw_payload"), F.lit("")), 256))
w = Window.partitionBy(dedupe_key).orderBy(
    F.col("created_at").desc_nulls_last(),
    F.col("ingestion_timestamp").desc()
)
flat_deduped = (
    flat
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

# ---------------- Write Silver (Parquet, partitioned by dt) ----------------
(flat_deduped
    .repartition(1, "dt")
    .write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(silver_path)
)

print("OK: Bronze → Silver complete.")
