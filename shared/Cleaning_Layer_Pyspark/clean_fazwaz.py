from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, when, lit

def main():
    spark = SparkSession.builder.appName("Clean_Fazwaz").getOrCreate()

    # Read Bronze
    df = spark.read.parquet("hdfs://namenode:9000/datalake/bronze/fazwaz_apartments_allcombined")

    # 1. Clean Numeric Columns
    df = df.withColumn("price", regexp_replace(col("price"), "[^0-9.]", "").cast("double")) \
           .withColumn("price_per_sqm", regexp_replace(col("price_per_sqm"), "[^0-9.]", "").cast("double")) \
           .withColumn("size", col("size").cast("double")) \
           .withColumn("bedrooms", col("bedrooms").cast("integer")) \
           .withColumn("bathrooms", col("bathrooms").cast("integer"))

    # 2. Map 'mawgood' to Binary Flags
    df = df.withColumn("Pool", when(lower(col("Private Pool")) == "mawgood", 1).otherwise(0)) \
           .withColumn("Garden", when(lower(col("Private Garden")) == "mawgood", 1).otherwise(0)) \
           .withColumn("Parking", when(lower(col("Covered Parking")) == "mawgood", 1).otherwise(0)) \
           .withColumn("Gym", when(lower(col("Private Gym")) == "mawgood", 1).otherwise(0)) \
           .withColumn("Maids_Quarters", when(lower(col("Maids Quarters")) == "mawgood", 1).otherwise(0)) \
           .withColumn("Jacuzzi", when(lower(col("Jacuzzi")) == "mawgood", 1).otherwise(0)) \
           .withColumn("Balcony", when(lower(col("Balcony")) == "mawgood", 1).otherwise(0)) \
           .withColumn("Spa", lit(0))

    # 3. Drop unused columns & duplicates
    df = df.drop("Private Pool", "Private Garden", "Covered Parking", "Private Gym", "Maids Quarters", "unit_id")
    df = df.na.drop(subset=["size"])
    df = df.dropDuplicates()

    # Write to Silver
    output_path = "hdfs://namenode:9000/datalake/silver/cleaned_fazwaz"
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Fazwaz cleaned data saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
