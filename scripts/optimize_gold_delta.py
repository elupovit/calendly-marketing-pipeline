from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# ---- CONFIG ----
GOLD_PATH = "s3://calendly-marketing-pipeline-data/gold/calendly/events/"
RETENTION_HOURS = 168  # 7 days

# ---- INIT GLUE CONTEXT ----
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

logger.info("🚀 Starting Gold Delta optimization...")

# ---- LOAD DELTA TABLE ----
dt = DeltaTable.forPath(spark, GOLD_PATH)

# ---- COMPACTION ----
logger.info("🔧 Compaction in progress...")
(
    dt.toDF()
      .repartition(1)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(GOLD_PATH)
)
logger.info("✅ Compaction complete.")

# ---- VACUUM ----
logger.info(f"🧹 Running VACUUM (retaining {RETENTION_HOURS} hours)...")
spark.sql(f"VACUUM delta.`{GOLD_PATH}` RETAIN {RETENTION_HOURS} HOURS")
logger.info("✅ Vacuum complete.")

# ---- VALIDATION ----
rows = spark.read.format("delta").load(GOLD_PATH).count()
logger.info(f"VALIDATION_OK: post-optimization row count = {rows}")
logger.info("🎯 Gold Delta optimization successful.")
