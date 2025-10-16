import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pydeequ.verification import VerificationSuite, Check, CheckLevel
from pydeequ.repository import FileSystemMetricsRepository, ResultKey
from pydeequ.analyzers import Size, Completeness, ApproxCountDistinct

# Glue & Spark setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()

# Paths
GOLD_PATH = "s3://calendly-marketing-pipeline-data/gold/calendly/events/"

# Read gold delta
logger.info("🔸 Loading Gold Delta table for quality validation...")
df = spark.read.format("delta").load(GOLD_PATH)
logger.info(f"✅ Loaded {df.count()} rows from Gold Delta table")

# Setup PyDeequ repository
repository_path = "s3://calendly-marketing-pipeline-data/quality-metrics/"
repository = FileSystemMetricsRepository(spark, repository_path)
resultKey = ResultKey(spark, {
    "jobName": args['JOB_NAME']
})

# Run checks
logger.info("🔍 Running data quality checks...")
check = (
    Check(spark, CheckLevel.Error, "GoldDataQuality")
    .hasSize(lambda x: x > 0, "Gold table should not be empty")
    .isComplete("event_type", lambda x: x >= 0.95, "Event type completeness")
    .hasApproxCountDistinct("event_type", lambda x: x > 1, "Event type diversity")
)

verificationResult = (
    VerificationSuite(spark)
    .onData(df)
    .useRepository(repository)
    .saveOrAppendResult(resultKey)
    .addCheck(check)
    .run()
)

if verificationResult.status == "Success":
    logger.info("✅ Data quality checks passed!")
else:
    logger.error("❌ Data quality checks failed — review PyDeequ metrics in S3")

job.commit()
