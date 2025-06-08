from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, countDistinct, isnan, mean, stddev, min, max, lit
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("Customer Table Profiling") \
    .enableHiveSupport() \
    .getOrCreate()

source_table = "app.customer"
output_table = "app.customer_profile"

df = spark.table(source_table)

# Get list of countries
countries = df.select("country").distinct().rdd.flatMap(lambda x: x).collect()

all_profiles = []

for country in countries:
    df_country = df.filter(col("country") == country)
    total_rows = df_country.count()
    
    for col_name, dtype in df_country.dtypes:
        if col_name == "country":
            continue
        
        col_data = df_country.select(col(col_name))
        null_count = df_country.filter(col(col_name).isNull() | isnan(col(col_name))).count()
        distinct_count = col_data.distinct().count()
        null_pct = null_count / total_rows if total_rows else None
        
        # Numeric stats
        if dtype in ["int", "double", "float", "bigint", "decimal"]:
            stats = df_country.select(
                mean(col(col_name)).alias("mean"),
                stddev(col(col_name)).alias("stddev"),
                min(col(col_name)).alias("min"),
                max(col(col_name)).alias("max")
            ).first()
            row = (
                country, col_name, dtype,
                null_count, null_pct, distinct_count,
                stats["mean"], stats["stddev"], stats["min"], stats["max"]
            )
        else:
            row = (
                country, col_name, dtype,
                null_count, null_pct, distinct_count,
                None, None, None, None
            )
        
        all_profiles.append(row)

# Convert to DataFrame
schema = ["country", "column", "type", "null_count", "null_pct", "distinct_count", "mean", "stddev", "min", "max"]
profile_df = spark.createDataFrame(all_profiles, schema)

# Save as Hive table
profile_df.write.mode("overwrite").saveAsTable(output_table)

# Save to HDFS as HTML-friendly CSV (can be served from web if needed)
profile_df.orderBy("country", "column") \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv("/tmp/customer_profile_report/")

print("✅ Profiling completed. Saved to Hive and /tmp/customer_profile_report/")
