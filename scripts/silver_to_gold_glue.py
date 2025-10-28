from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import DoubleType
import logging
import datetime

# =========================================================
# CONFIG
# =========================================================
silver_events_path = "s3://calendly-marketing-pipeline-data/silver/calendly/events/"
silver_spend_path  = "s3://calendly-marketing-pipeline-data/silver/marketing_spend/"

gold_base_path = "s3://calendly-marketing-pipeline-data/gold/calendly/"
gold_daily_calls_path   = f"{gold_base_path}bookings_by_source_day/"
gold_cpb_path           = f"{gold_base_path}cost_per_booking_by_channel/"
gold_trend_path         = f"{gold_base_path}bookings_trend_over_time/"
gold_attribution_path   = f"{gold_base_path}channel_attribution_leaderboard/"
gold_timeslot_path      = f"{gold_base_path}bookings_by_timeslot/"
gold_employee_path      = f"{gold_base_path}meetings_per_employee_week/"

DELTA_PACKAGE = "io.delta:delta-core_2.12:2.3.0"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver-to-gold-odate")

# =========================================================
# CALCULATE ODATE (yesterday)
# =========================================================
odate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
logger.info(f"🗓️  Processing odate: {odate}")

# =========================================================
# SPARK SESSION
# =========================================================
spark = (
    SparkSession.builder.appName("SilverToGold_ODate")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.jars.packages", DELTA_PACKAGE)
    .getOrCreate()
)

# =========================================================
# READ SILVER PARTITIONS
# =========================================================
silver_events_part = f"{silver_events_path.rstrip('/')}/dt={odate}/"
silver_spend_part  = f"{silver_spend_path.rstrip('/')}/dt={odate}/"

try:
    df_events = spark.read.format("delta").load(silver_events_part).dropDuplicates()
except Exception as e:
    logger.error(f"⚠️ Could not read Delta for events {silver_events_part}: {e}")
    df_events = spark.read.option("multiline", "true").json(silver_events_part).dropDuplicates()

try:
    df_spend = spark.read.format("delta").load(silver_spend_part).dropDuplicates()
except Exception as e:
    logger.warning(f"⚠️ No spend data found for {odate}: {e}")
    df_spend = spark.createDataFrame([], schema="date string, channel string, spend double, dt string")

# =========================================================
# NORMALIZE CHANNELS / SOURCES
# =========================================================
df_events = (
    df_events
    .withColumn("channel", F.lower(F.coalesce(F.col("channel"), F.lit("unknown"))))
)

df_spend = (
    df_spend
    .withColumn(
        "channel_norm",
        F.when(F.col("channel") == "facebook_paid_ads", "facebook")
         .when(F.col("channel") == "youtube_paid_ads",  "youtube")
         .when(F.col("channel") == "tiktok_paid_ads",   "tiktok")
         .otherwise(F.lower(F.col("channel")))
    )
    .withColumn("spend_date", F.to_date("date"))
)

spend_agg = (
    df_spend.groupBy("spend_date", "channel_norm")
    .agg(F.sum("spend").alias("spend_total"))
    .withColumnRenamed("channel_norm", "spend_channel")
)

# =========================================================
# 1.1 DAILY CALLS BOOKED BY SOURCE (now uses channel)
# =========================================================
daily_calls = (
    df_events
    .groupBy("booking_date", "channel")
    .agg(F.countDistinct("scheduled_event_uri").alias("bookings"))
    .select("booking_date", F.col("channel").alias("source"), F.col("bookings").alias("booking_id"))
    .withColumn("dt", F.lit(odate))
)
logger.info("✅ 1.1 Daily Calls Booked by Source created")

# =========================================================
# 1.2 COST PER BOOKING (CPB) BY CHANNEL — OVERALL
# =========================================================
bookings_per_channel = (
    df_events
    .groupBy("channel")
    .agg(F.countDistinct("scheduled_event_uri").alias("total_bookings"))
)

spend_per_channel = (
    df_spend
    .withColumn("channel_norm",
        F.when(F.col("channel") == "facebook_paid_ads", "facebook")
         .when(F.col("channel") == "youtube_paid_ads",  "youtube")
         .when(F.col("channel") == "tiktok_paid_ads",   "tiktok")
         .otherwise(F.lower(F.col("channel")))
    )
    .groupBy("channel_norm")
    .agg(F.sum("spend").alias("total_spend"))
    .withColumnRenamed("channel_norm", "channel")
)

cpb_by_channel = (
    bookings_per_channel
    .join(spend_per_channel, "channel", "left")
    .withColumn("total_spend", F.coalesce(F.col("total_spend"), F.lit(0.0)))
    .withColumn("cpb", F.when(F.col("total_bookings") > 0, F.col("total_spend") / F.col("total_bookings")).otherwise(None))
    .select("channel", "total_spend", "total_bookings", "cpb")
    .withColumn("dt", F.lit(odate))
)
logger.info("✅ 1.2 Cost Per Booking (overall) created")

# =========================================================
# 1.3 BOOKINGS TREND OVER TIME
# =========================================================
bookings_trend = (
    df_events
    .groupBy("booking_date", "channel")
    .agg(F.countDistinct("scheduled_event_uri").alias("total_bookings"))
    .select("booking_date", F.col("channel").alias("source"), F.col("total_bookings").alias("booking_id"))
    .withColumn("dt", F.lit(odate))
)
logger.info("✅ 1.3 Bookings Trend Over Time created")

# =========================================================
# 1.4 CHANNEL ATTRIBUTION (CPB & VOLUME LEADERBOARD)
# =========================================================
spend_summary = (
    spend_agg.groupBy("spend_channel")
    .agg(F.sum("spend_total").alias("total_spend"))
)

channel_attr = (
    df_events
    .groupBy("channel", "event_type_id")
    .agg(F.countDistinct("scheduled_event_uri").alias("total_bookings"))
    .join(spend_summary, F.col("channel") == F.col("spend_channel"), "left")
    .withColumn("total_spend", F.coalesce(F.col("total_spend"), F.lit(0.0)))
    .withColumn("cpb", F.when(F.col("total_bookings") > 0, F.col("total_spend") / F.col("total_bookings")).otherwise(None))
    .withColumnRenamed("event_type_id", "campaign")
    .select(F.col("channel").alias("source"), "campaign", "total_bookings", F.col("total_spend").alias("spend"), "cpb")
    .withColumn("dt", F.lit(odate))
)
logger.info("✅ 1.4 Channel Attribution Leaderboard created")

# =========================================================
# 1.5 BOOKING VOLUME BY TIME SLOT / DAY OF WEEK
# =========================================================
bookings_timeslot = (
    df_events
    .withColumn("event_start_local_ts", F.to_timestamp("event_start_local"))
    .withColumn("hour_of_day", F.hour("event_start_local_ts"))
    .withColumn("day_of_week", F.date_format("event_start_local_ts", "E"))
    .groupBy("hour_of_day", "day_of_week", "channel")
    .agg(F.countDistinct("scheduled_event_uri").alias("bookings"))
    .select("hour_of_day", "day_of_week", F.col("channel").alias("source"), F.col("bookings").alias("booking_id"))
    .withColumn("dt", F.lit(odate))
)
logger.info("✅ 1.5 Booking Volume by Time Slot / Day of Week created")

# =========================================================
# 1.6 MEETINGS PER EMPLOYEE (weekly)
# =========================================================
df_events = df_events.withColumn("week_of_year", F.weekofyear("booking_date"))
df_events = df_events.withColumn("year", F.year("booking_date"))

meetings_per_emp = (
    df_events.groupBy("host_user_name", "year", "week_of_year")
    .agg(F.countDistinct("scheduled_event_uri").alias("meetings"))
    .withColumn("avg_meetings_per_week", F.col("meetings").cast(DoubleType()))
    .withColumn("dt", F.lit(odate))
)

logger.info("✅ 1.6 Meetings per Employee created and casted to DoubleType for Athena schema compatibility")


# =========================================================
# WRITE TO GOLD (DELTA with fallback)
# =========================================================
def write_delta(df, path, name):
    try:
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("dt") \
            .save(path)
        logger.info(f"✅ Wrote {name} to {path}")
    except Exception as e:
        logger.warning(f"⚠️ Delta write failed for {name}: {e}. Falling back to Parquet.")
        df.write.mode("overwrite").partitionBy("dt").parquet(path)
        logger.info(f"✅ Wrote {name} as Parquet to {path}")

# =========================================================
# WRITE ALL GOLD TABLES
# =========================================================
write_delta(daily_calls,       gold_daily_calls_path, "bookings_by_source_day")
write_delta(cpb_by_channel,    gold_cpb_path,         "cost_per_booking_by_channel")
write_delta(bookings_trend,    gold_trend_path,       "bookings_trend_over_time")
write_delta(channel_attr,      gold_attribution_path, "channel_attribution_leaderboard")
write_delta(bookings_timeslot, gold_timeslot_path,    "bookings_by_timeslot")
write_delta(meetings_per_emp,  gold_employee_path,    "meetings_per_employee_week")

logger.info(f"🎉 Silver → Gold completed successfully for {odate}!")
spark.stop()
