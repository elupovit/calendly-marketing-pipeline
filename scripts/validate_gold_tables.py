# validate_gold_tables.py  (Glue ETL job, Spark 3.x + PyDeequ)
import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pydeequ.verification import VerificationSuite, Check, CheckLevel
from pydeequ.repository import FileSystemMetricsRepository, ResultKey

sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
log = glue.get_logger()

GOLD = "s3://calendly-marketing-pipeline-data/gold/calendly/"

TABLES = {
    "gold_bookings_by_source_day": {
        "path": f"{GOLD}bookings_by_source_day/",
        "required": ["booking_date","source","booking_id","dt"],
        "not_null": ["booking_date","source","booking_id","dt"],
        "unique_combo": ["booking_date","source","booking_id"],  # light uniqueness
    },
    "gold_bookings_trend_over_time": {
        "path": f"{GOLD}bookings_trend_over_time/",
        "required": ["booking_date","source","booking_id","dt"],
        "not_null": ["booking_date","source","booking_id","dt"],
    },
    "gold_bookings_by_timeslot": {
        "path": f"{GOLD}bookings_by_timeslot/",
        "required": ["hour_of_day","day_of_week","source","booking_id","dt"],
        "not_null": ["hour_of_day","day_of_week","source","booking_id","dt"],
    },
    "gold_cost_per_booking_by_channel": {
        "path": f"{GOLD}cost_per_booking_by_channel/",
        "required": ["channel","total_bookings","total_spend","cpb","dt"],
        "not_null": ["channel","dt"],
    },
    "gold_channel_attribution_leaderboard": {
        "path": f"{GOLD}channel_attribution_leaderboard/",
        "required": ["source","campaign","total_bookings","spend","cpb","dt"],
        "not_null": ["source","campaign","dt"],
    },
    "gold_meetings_per_employee_week": {
        "path": f"{GOLD}meetings_per_employee_week/",
        "required": ["host_user_name","year","week_of_year","meetings","avg_meetings_per_week","dt"],
        "not_null": ["host_user_name","year","week_of_year","dt"],
    },
}

# by default validate the most recent dt we see in the largest table
probe = spark.read.parquet(TABLES["gold_bookings_by_source_day"]["path"])
latest_dt = probe.select(F.max("dt").alias("dt")).collect()[0]["dt"]
log.info(f"Validating gold tables for dt={latest_dt}")

repo_path = "s3://calendly-marketing-pipeline-data/quality-metrics/"
repository = FileSystemMetricsRepository(spark, repo_path)
result_key = ResultKey(spark, {"runDt": latest_dt})

def run_checks(name, spec):
    log.info(f"[{name}] loading…")
    df_all = spark.read.parquet(spec["path"])
    df = df_all.where(F.col("dt") == latest_dt)

    if df.rdd.isEmpty():
        raise RuntimeError(f"[{name}] is empty for dt={latest_dt}")

    # Column presence
    missing = [c for c in spec["required"] if c not in df.columns]
    if missing:
        raise RuntimeError(f"[{name}] missing columns: {missing}")

    # Build Deequ checks
    chk = Check(spark, CheckLevel.Error, f"{name}-checks").hasSize(lambda x: x > 0)

    for c in spec.get("not_null", []):
        chk = chk.isComplete(c, lambda r: r >= 0.999)

    # sanity: numeric non-negatives where present
    for numc in ["booking_id","total_bookings","total_spend","cpb","meetings","avg_meetings_per_week"]:
        if numc in df.columns:
            chk = chk.isNonNegative(numc)

    # CPB ~= spend/bookings tolerance (where both exist)
    if {"spend","total_bookings","cpb"}.issubset(df.columns):
        df = df.withColumn("cpb_calc", F.when(F.col("total_bookings") > 0,
                                              F.col("spend") / F.col("total_bookings")).otherwise(None))
        # write temp view for analyzer-style comparison
        df.createOrReplaceTempView(f"vw_{name}")
        # A simple SQL assert via count of rows outside tolerance
        bad = spark.sql(f"""
            SELECT COUNT(*) AS bad
            FROM vw_{name}
            WHERE cpb IS NOT NULL AND cpb_calc IS NOT NULL
              AND ABS(cpb - cpb_calc) > 0.01
        """).collect()[0]["bad"]
        if bad > 0:
            raise RuntimeError(f"[{name}] CPB mismatch rows={bad}")

    vr = (VerificationSuite(spark)
          .onData(df)
          .useRepository(repository)
          .saveOrAppendResult(result_key)
          .addCheck(chk)
          .run())

    status = str(vr.status)
    if status != "Success":
        raise RuntimeError(f"[{name}] quality checks FAILED")
    log.info(f"[{name}] quality checks passed")

for n, s in TABLES.items():
    run_checks(n, s)

log.info("✅ All gold validations passed")
