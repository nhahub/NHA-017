from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, lit, split, element_at, trim, coalesce, concat_ws
from pyspark.sql.types import StringType

def main():
    spark = SparkSession.builder.appName("Transform_Dubbizle").getOrCreate()

    # Read Silver Data
    df_cairo = spark.read.parquet("hdfs://namenode:9000/datalake/silver/cleaned_dubbizle_cairo")
    df_alex = spark.read.parquet("hdfs://namenode:9000/datalake/silver/cleaned_dubbizle_alexandria")
    df = df_cairo.unionByName(df_alex, allowMissingColumns=True)

    # 1. Standardize
    if "area" in df.columns: df = df.withColumnRenamed("area", "size")
    if "description" in df.columns: df = df.withColumnRenamed("description", "title") # Dubbizle swap
    
    if "url" in df.columns: df = df.withColumnRenamed("url", "link")
    elif "link" not in df.columns: df = df.withColumn("link", lit(None).cast(StringType()))

    df = df.withColumn("source", lit("Dubbizle"))

    # 2. Description & Price/Sqm
    # Dubbizle often puts the long text in 'title' after we renamed it above. 
    # We ensure 'description' exists for the search.
    if "description" not in df.columns:
        df = df.withColumn("description", col("title"))

    df = df.withColumn("price_per_sqm", 
                       when(col("size") > 0, col("price") / col("size"))
                       .otherwise(lit(None)))

    # 3. Amenities Extraction
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

    # 4. Compound & Type
    compounds = ["hyde park", "mountain view", "palm hills", "mivida", "madinaty", "rehab", "eastown", "celia", "badya"]
    compound_expr = when(lit(False), lit(None))
    for cmp in compounds:
        compound_expr = compound_expr.when(col("text_search").contains(cmp), lit(cmp))
    df = df.withColumn("compound", compound_expr.otherwise("Unknown"))
    
    if "property_type" not in df.columns:
        df = df.withColumn("property_type", lit("Apartment"))

    target_cols = [
        "title", "price", "location", "size", "bedrooms", "bathrooms", 
        "compound", "source", "property_type", "link", "description", "price_per_sqm",
        "Jacuzzi", "Garden", "Balcony", "Pool", "Parking", "Gym", "Maids_Quarters", "Spa"
    ]
    selected_cols = [c for c in target_cols if c in df.columns]
    df = df.select(*selected_cols)

    output_path = "hdfs://namenode:9000/datalake/gold/dubbizle_gold"
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Dubbizle Gold data (Enriched) saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
