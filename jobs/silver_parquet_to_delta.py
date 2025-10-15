from pyspark.sql import SparkSession
from pyspark.sql import functions as F

source_parquet = "s3://calendly-marketing-pipeline-data/silver/calendly/events/"
target_delta   = "s3://calendly-marketing-pipeline-data/silver_delta/calendly/events/"
database = "calendly_marketing"
table    = "silver_calendly_events_delta"

spark = (SparkSession.builder.appName("silver_parquet_to_delta")
         .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
         .getOrCreate())

df = spark.read.parquet(source_parquet)
if "dt" not in df.columns:
    raise ValueError("Expected column 'dt' not found in silver Parquet.")

df = df.withColumn("dt", F.col("dt").cast("string"))

(df.repartition("dt")
   .write.format("delta")
   .mode("overwrite")
   .partitionBy("dt")
   .save(target_delta))

# Register/point Glue table to this Delta location
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database}.{table}
USING DELTA
LOCATION '{target_delta}'
""")

spark.stop()
