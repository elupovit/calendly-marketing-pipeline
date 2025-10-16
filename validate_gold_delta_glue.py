from pyspark.sql import SparkSession

GOLD_PATH = "s3://calendly-marketing-pipeline-data/gold/calendly/events/"

spark = (
    SparkSession.builder
    .appName("validate-gold-delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df = spark.read.format("delta").load(GOLD_PATH)

rows = df.count()
cols = len(df.columns)

print(f"VALIDATION_OK rows={rows} cols={cols}")
df.printSchema()
df.show(5, truncate=False)
