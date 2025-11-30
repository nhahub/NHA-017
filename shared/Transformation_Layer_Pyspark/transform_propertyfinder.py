from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, lit, coalesce, concat_ws
from pyspark.sql.types import StringType

def main():
    spark = SparkSession.builder.appName("Transform_PropertyFinder").getOrCreate()

    df = spark.read.parquet("hdfs://namenode:9000/datalake/silver/cleaned_propertyfinder")

    if "property_size" in df.columns: df = df.withColumnRenamed("property_size", "size")

    if "share_url" in df.columns: df = df.withColumnRenamed("share_url", "link")
    elif "url" in df.columns: df = df.withColumnRenamed("url", "link")
    elif "link" not in df.columns: df = df.withColumn("link", lit(None).cast(StringType()))

    # Lat/Long check
    if "lat" in df.columns: df = df.withColumnRenamed("lat", "latitude")
    if "lon" in df.columns: df = df.withColumnRenamed("lon", "longitude")

    df = df.withColumn("source", lit("PropertyFinder"))

    # Description & Price/Sqm
    if "description" not in df.columns:
        df = df.withColumn("description", col("title"))
    else:
        df = df.withColumn("description", coalesce(col("description"), col("title")))

    df = df.withColumn("price_per_sqm", 
                       when(col("size") > 0, col("price") / col("size"))
                       .otherwise(lit(None)))

    # Amenities
    df = df.withColumn("text_search", lower(concat_ws(" ", col("title"), col("description"))))
    
    amenities = {
        "Jacuzzi": ["jacuzzi"], "Garden": ["garden"], "Balcony": ["balcony", "terrace"],
        "Pool": ["pool", "swimming"], "Parking": ["parking", "garage"], "Gym": ["gym"],
        "Maids_Quarters": ["maid"], "Spa": ["spa", "sauna"]
    }

    for col_name, keywords in amenities.items():
        condition = lit(False)
        for kw in keywords:
            condition = condition | col("text_search").contains(kw)
        df = df.withColumn(col_name, when(condition, 1).otherwise(0))

    # Compound
    compounds = ["hyde park", "mountain view", "palm hills", "mivida", "madinaty", "rehab", "eastown", "celia", "badya"]
    compound_expr = when(lit(False), lit(None))
    for cmp in compounds:
        compound_expr = compound_expr.when(col("text_search").contains(cmp), lit(cmp))
    df = df.withColumn("compound", compound_expr.otherwise("Unknown"))

    # Target Columns (Includes Lat/Long and Amenities)
    target_cols = [
        "title", "price", "location", "size", "bedrooms", "bathrooms", 
        "compound", "source", "property_type", "link", "description", "price_per_sqm",
        "latitude", "longitude",
        "Jacuzzi", "Garden", "Balcony", "Pool", "Parking", "Gym", "Maids_Quarters", "Spa"
    ]
    selected_cols = [c for c in target_cols if c in df.columns]
    df = df.select(*selected_cols)

    output_path = "hdfs://namenode:9000/datalake/gold/propertyfinder_gold"
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ PropertyFinder Gold data (Enriched) saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
