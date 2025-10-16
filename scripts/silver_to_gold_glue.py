# scripts/silver_to_gold_glue.py
# Read Silver (Delta) -> build/merge Gold (Delta). Idempotent via event_uuid.

import sys
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

# ---------------- Params ----------------
args = {a.split("=",1)[0]: a.split("=",1)[1] for a in sys.argv[1:] if "=" in a}
SILVER_PATH = args.get("silver_path", "s3://calendly-marketing-pipeline-data/silver_delta/calendly/events/")
GOLD_PATH   = args.get("gold_path",   "s3://calendly-marketing-pipeline-data/gold/calendly/events/")

# ---------------- Spark -----------------
spark = (
    SparkSession.builder
      .appName("calendly_silver_to_gold_delta")
      # Delta engine bits (works on Glue 4.0)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .config("spark.sql.session.timeZone","UTC")
      .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

print(f"🔧 silver_path={SILVER_PATH}")
print(f"🔧 gold_path={GOLD_PATH}")

# ---------------- Load Silver (Delta) ----------------
silver = spark.read.format("delta").load(SILVER_PATH)

# Minimal projection/cleansing for Gold
gold_upserts = (
    silver.select(
        "event_uuid",
        "event_type",
        "event_type_name",
        "invitee_name",
        "invitee_email",
        "event_start_time",
        "event_end_time",
        "created_at",
        "dt"
    )
    .dropDuplicates(["event_uuid"])  # safety
)

print(f"📦 incoming rows = {gold_upserts.count()}")

# ---------------- Merge into Gold (Delta) ----------------
def ensure_gold_exists():
    try:
        _ = DeltaTable.forPath(spark, GOLD_PATH)
        return True
    except Exception:
        return False

if ensure_gold_exists():
    print("🟡 Gold Delta exists -> MERGE (upsert)")
    tgt = DeltaTable.forPath(spark, GOLD_PATH)
    # Upsert on event_uuid
    (
        tgt.alias("t")
           .merge(
               gold_upserts.alias("s"),
               "t.event_uuid = s.event_uuid"
           )
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )
else:
    print("🟢 Gold Delta not found -> initial CREATE (overwrite)")
    (
        gold_upserts
          .repartition(1, "dt")           # gentle compaction by partition
          .write
          .format("delta")
          .mode("overwrite")
          .partitionBy("dt")
          .save(GOLD_PATH)
    )

print("✅ Merge/create complete.")

# ---------------- (Optional) Register external table ----------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS gold_calendly_events
USING DELTA
LOCATION '{GOLD_PATH}'
""")

# ---------------- Light maintenance ----------------
# NOTE: Databricks-only OPTIMIZE/ZORDER won't run on Glue. We can VACUUM safely.
spark.sql(f"VACUUM delta.`{GOLD_PATH}` RETAIN 168 HOURS")  # 7 days

# ---------------- Sanity checks ----------------
print("🔍 Sanity checks on Gold...")
gold_tbl = DeltaTable.forPath(spark, GOLD_PATH)
gold_df  = gold_tbl.toDF()

print(f"Gold row count: {gold_df.count()}")
gold_df.groupBy("dt").count().orderBy("dt").show(truncate=False)

print("🎉 Silver -> Gold Delta sync finished.")
