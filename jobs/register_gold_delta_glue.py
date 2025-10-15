from pyspark.sql import SparkSession

# Initialize Spark session with Delta + Glue support
spark = (
    SparkSession.builder.appName("register_gold_delta_glue")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

# Define Gold Delta path and Glue Catalog target
gold_path = "s3://calendly-marketing-pipeline-data/gold/calendly/events/"
database_name = "calendly_marketing"
table_name = "gold_calendly_events_delta"

# Register Delta table in Glue Catalog
spark.sql(f"""
CREATE DATABASE IF NOT EXISTS {database_name}
LOCATION 's3://calendly-marketing-pipeline-data/gold_delta/'
""")

spark.sql(f"""
CREATE TABLE {database_name}.{table_name}
USING DELTA
LOCATION '{gold_path}'
""")

print(f"✅ Registered Delta table: {database_name}.{table_name}")
spark.stop()
