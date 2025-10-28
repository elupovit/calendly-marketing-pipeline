# optimize_gold_parquet.py  (Glue ETL job, Spark 3.x)
import datetime
from pyspark.sql import SparkSession, functions as F

GOLD_BASE = "s3://calendly-marketing-pipeline-data/gold/calendly/"
TABLES = {
    "bookings_by_source_day":      f"{GOLD_BASE}bookings_by_source_day/",
    "bookings_trend_over_time":    f"{GOLD_BASE}bookings_trend_over_time/",
    "bookings_by_timeslot":        f"{GOLD_BASE}bookings_by_timeslot/",
    "cost_per_booking_by_channel": f"{GOLD_BASE}cost_per_booking_by_channel/",
    "channel_attribution_leaderboard": f"{GOLD_BASE}channel_attribution_leaderboard/",
    "meetings_per_employee_week":  f"{GOLD_BASE}meetings_per_employee_week/",
}

spark = (
    SparkSession.builder.appName("optimize-gold-parquet")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

# choose the partition to compact; default = yesterday (same as your s→g job)
odate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

def compact_one(tbl_name, path, target_files=2):
    df = spark.read.parquet(path).where(F.col("dt") == odate)
    if df.rdd.isEmpty():
        print(f"[{tbl_name}] no data for dt={odate}, skipping")
        return
    # Heuristic: coalesce to small number of files; adjust if partitions grow
    out = df.coalesce(target_files)
    (
        out.write.mode("overwrite")
           .partitionBy("dt")
           .parquet(path)
    )
    print(f"[{tbl_name}] compacted dt={odate} -> ~{target_files} files")

for name, p in TABLES.items():
    compact_one(name, p)

spark.stop()
