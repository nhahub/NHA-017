from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim

def main():
    spark = SparkSession.builder.appName("Clean_Dubbizle").getOrCreate()

    # Read Bronze (Cairo & Alex)
    df_cairo = spark.read.parquet("hdfs://namenode:9000/datalake/bronze/dubizzle_all_listings_cairo")
    df_alex = spark.read.parquet("hdfs://namenode:9000/datalake/bronze/dubbizle_alexandria")

    def clean_dubbizle_df(df):
        # 1. Remove duplicates & nulls
        df = df.dropDuplicates().dropna()
        
        # 2. Clean Price (remove symbols, cast to double)
        df = df.withColumn("price", regexp_replace(col("price"), "[^0-9.]", "").cast("double"))
        
        # 3. Clean Area (remove 'SQM', cast to double)
        df = df.withColumn("area", regexp_replace(col("area"), "SQM|sqm|\\s", "").cast("double"))
        
        # 4. Cast Beds/Baths
        df = df.withColumn("bedrooms", col("bedrooms").cast("integer")) \
               .withColumn("bathrooms", col("bathrooms").cast("integer"))
        
        return df

    # Apply cleaning
    df_cairo_cleaned = clean_dubbizle_df(df_cairo)
    df_alex_cleaned = clean_dubbizle_df(df_alex)

    # Write to Silver
    df_cairo_cleaned.write.mode("overwrite").parquet("hdfs://namenode:9000/datalake/silver/cleaned_dubbizle_cairo")
    df_alex_cleaned.write.mode("overwrite").parquet("hdfs://namenode:9000/datalake/silver/cleaned_dubbizle_alexandria")
    
    print("✅ Dubbizle cleaned data saved to Silver layer")

    spark.stop()

if __name__ == "__main__":
    main()
