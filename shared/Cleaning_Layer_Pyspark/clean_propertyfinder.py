from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, trim, regexp_replace

def main():
    spark = SparkSession.builder.appName("Clean_PropertyFinder").getOrCreate()

    # Read Bronze
    df = spark.read.parquet("hdfs://namenode:9000/datalake/bronze/propertyfinder")

    # 1. Filter valid prices
    df = df.filter((col("price").isNotNull()) & (col("price") > 0))

    # 2. Extract Amenity Flags
    df = df.withColumn("Pool", when(lower(col("amenities")).contains("pool") | lower(col("amenities")).contains("swimming"), 1).otherwise(0)) \
           .withColumn("Gym", when(lower(col("amenities")).contains("gym") | lower(col("amenities")).contains("fitness") | lower(col("amenities")).contains("workout"), 1).otherwise(0)) \
           .withColumn("Garden", when(lower(col("amenities")).contains("garden") | lower(col("amenities")).contains("gardening") | lower(col("amenities")).contains("lawn"), 1).otherwise(0)) \
           .withColumn("Parking", when(lower(col("amenities")).contains("parking") | lower(col("amenities")).contains("garage") | lower(col("amenities")).contains("carport"), 1).otherwise(0)) \
           .withColumn("Maids_Quarters", when(lower(col("amenities")).contains("maids") | lower(col("amenities")).contains("servant") | lower(col("amenities")).contains("staff"), 1).otherwise(0)) \
           .withColumn("Jacuzzi", when(lower(col("amenities")).contains("jacuzzi"), 1).otherwise(0)) \
           .withColumn("Balcony", when(lower(col("amenities")).contains("balcony"), 1).otherwise(0)) \
           .withColumn("Spa", when(lower(col("amenities")).contains("spa"), 1).otherwise(0))

    # 3. Clean Numeric Columns & Casting
    df = df.withColumn("price", col("price").cast("double")) \
           .withColumn("property_size", regexp_replace(col("property_size"), "[^0-9.]", "").cast("double")) \
           .withColumn("bedrooms", regexp_replace(col("bedrooms"), "[^0-9]", "").cast("int")) \
           .withColumn("bathrooms", regexp_replace(col("bathrooms"), "[^0-9]", "").cast("int"))

    # 4. Calculate Derived Columns & Final Cleanup
    df = df.filter(col("property_size") > 0)
    df = df.withColumn("price_per_sqm", col("price") / col("property_size"))
    df = df.withColumn("amenities", trim(lower(col("amenities"))))
    
    # Drop unnecessary columns
    df = df.drop("id", "currency")

    # Write to Silver
    output_path = "hdfs://namenode:9000/datalake/silver/cleaned_propertyfinder"
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ PropertyFinder cleaned data saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
