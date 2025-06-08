from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, isnan, when, mean, stddev, min, max, lit
)
import pandas as pd
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder \
    .appName("Customer Table Profiler") \
    .enableHiveSupport() \
    .getOrCreate()

# Load table
table = "app.customer"
df = spark.table(table)

# Get list of numeric columns
numeric_types = ["int", "bigint", "float", "double", "decimal"]
columns = df.dtypes

# List of countries
countries = [row["country"] for row in df.select("country").distinct().collect()]

# Final profiling result
all_profiles = []

for country in countries:
    df_country = df.filter(col("country") == country)
    total_rows = df_country.count()

    for col_name, dtype in columns:
        if col_name == "country":
            continue  # skip country column since it's grouping key

        null_count = df_country.filter(col(col_name).isNull() | isnan(col(col_name))).count()
        null_pct = null_count / total_rows if total_rows > 0 else None
        distinct_count = df_country.select(col_name).distinct().count()

        profile = {
            "country": country,
            "column": col_name,
            "type": dtype,
            "null_count": null_count,
            "null_pct": round(null_pct, 4) if null_pct is not None else None,
            "distinct_count": distinct_count,
        }

        if dtype in numeric_types:
            stats = df_country.select(
                mean(col(col_name)).alias("mean"),
                stddev(col(col_name)).alias("stddev"),
                min(col(col_name)).alias("min"),
                max(col(col_name)).alias("max")
            ).first()

            profile["mean"] = round(stats["mean"], 2) if stats["mean"] is not None else None
            profile["stddev"] = round(stats["stddev"], 2) if stats["stddev"] is not None else None
            profile["min"] = stats["min"]
            profile["max"] = stats["max"]
        else:
            profile["mean"] = None
            profile["stddev"] = None
            profile["min"] = None
            profile["max"] = None

        all_profiles.append(profile)

# Convert to DataFrame
profile_df = spark.createDataFrame(all_profiles)

# Save to Hive table (overwrite daily)
profile_df.write.mode("overwrite").saveAsTable("app.customer_profile")

# Save as HTML report
local_df = profile_df.toPandas()
today = datetime.today().strftime("%Y-%m-%d")
html_path = f"/tmp/customer_profile_report_{today}.html"
local_df.to_html(html_path, index=False, border=0, justify="center")

print(f"✔ Profile saved to Hive table: app.customer_profile")
print(f"✔ HTML report saved to: {html_path}")
