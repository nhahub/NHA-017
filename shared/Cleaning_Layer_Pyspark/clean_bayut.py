from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, when
from pyspark.sql.types import DoubleType, IntegerType

def main():
    spark = SparkSession.builder.appName("Clean_Bayut").getOrCreate()

    # Read Bronze Data
    df = spark.read.parquet("hdfs://namenode:9000/datalake/bronze/bayutData")

    # Debug: Print schema to see actual columns in logs
    print("Input Schema:")
    df.printSchema()

    # 1. Normalize Property Type
    df = df.withColumn("property_type",
        when(lower(col("title")).like("%apartment%") | lower(col("title")).like("%apt%") | lower(col("title")).contains("apartm"), "Apartment")
        .when(lower(col("title")).like("%townhouse%") | lower(col("title")).contains("town"), "Townhouse")
        .when(lower(col("title")).like("%twin house%") | lower(col("title")).like("%twin%"), "Twin House")
        .when(lower(col("title")).like("%villa%"), "Villa")
        .when(lower(col("title")).like("%ivilla%"), "iVilla")
        .when(lower(col("title")).like("%chalet%"), "Chalet")
        .when(lower(col("title")).like("%duplex%"), "Duplex")
        .when(lower(col("title")).like("%penthouse%"), "Penthouse")
        .otherwise("Other")
    )

    # 2. Basic Cleanup
    df = df.dropDuplicates()
    df = df.na.drop(subset=["title", "price"])
    
    for c in df.columns:
        df = df.withColumn(c, trim(col(c)))

    # 3. Numeric Casting (UPDATED: 'size_sqm' -> 'area_sqm')
    numeric_cols = {
        "price": IntegerType(),
        "area_sqm": DoubleType(), # <--- Fixed: Matches new data format
        "bedrooms": IntegerType(),
        "bathrooms": IntegerType(),
        "latitude": DoubleType(),
        "longitude": DoubleType()
    }

    for col_name, dtype in numeric_cols.items():
        # Only process if the column actually exists to avoid crashing
        if col_name in df.columns:
            df = df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9.-]", "").cast(dtype))

    # 4. Final Formatting
    string_cols = ["title", "url", "location", "region", "property_type"]
    for col_name in string_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(lower(col(col_name))))

    # 5. Select Final Columns
    # We define the target list, but only select what actually exists
    target_cols = ["title", "url", "price", "location", "region", 
                   "area_sqm", "bedrooms", "bathrooms", "latitude", "longitude", "property_type"]
    
    existing_cols = [c for c in target_cols if c in df.columns]
    df = df.select(*existing_cols)

    # Write to Silver
    output_path = "hdfs://namenode:9000/datalake/silver/cleaned_bayut"
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Bayut cleaned data saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
