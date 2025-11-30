from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, lit, split, element_at, trim, regexp_replace, coalesce, concat_ws
from pyspark.sql.types import StringType, IntegerType, DoubleType

def main():
    spark = SparkSession.builder.appName("Transform_Bayut").getOrCreate()

    # Read Silver Data
    df = spark.read.parquet("hdfs://namenode:9000/datalake/silver/cleaned_bayut")

    # --- 1. Standardize Core Columns ---
    if "area_sqm" in df.columns:
        df = df.withColumnRenamed("area_sqm", "size")
    elif "size_sqm" in df.columns:
        df = df.withColumnRenamed("size_sqm", "size")

    # Link Fix
    if "url" in df.columns:
        df = df.withColumnRenamed("url", "link")
    elif "link" not in df.columns:
        df = df.withColumn("link", lit(None).cast(StringType()))
    
    # Source
    df = df.withColumn("source", lit("Bayut"))

    # --- 2. Location Logic (City/Region Split) ---
    df = df.withColumn("loc_clean", regexp_replace(col("location"), "[.]+$", ""))
    df = df.withColumn("loc_parts", split(col("loc_clean"), ","))
    df = df.withColumn("city", trim(element_at(col("loc_parts"), -1)))
    df = df.withColumn("region", trim(element_at(col("loc_parts"), -2)))
    df = df.withColumn("region", when(col("region").isNull(), col("city")).otherwise(col("region")))
    df = df.drop("loc_clean", "loc_parts")

    # --- 3. Description & Price Per Sqm Fix ---
    
    # If description is missing, use Title
    if "description" not in df.columns:
        df = df.withColumn("description", col("title"))
    else:
        df = df.withColumn("description", coalesce(col("description"), col("title")))

    # Compute Price Per Sqm
    df = df.withColumn("price_per_sqm", 
                       when(col("size") > 0, col("price") / col("size"))
                       .otherwise(lit(None)))

    # --- 4. Amenities Extraction (The AI Logic) ---
    # Create a text field to search in (Title + Description)
    df = df.withColumn("text_search", lower(concat_ws(" ", col("title"), col("description"))))

    amenities = {
        "Jacuzzi": ["jacuzzi"],
        "Garden": ["garden"],
        "Balcony": ["balcony", "terrace"],
        "Pool": ["pool", "swimming"],
        "Parking": ["parking", "garage"],
        "Gym": ["gym"],
        "Maids_Quarters": ["maid"],
        "Spa": ["spa", "sauna"]
    }

    for col_name, keywords in amenities.items():
        # Build OR condition: if text contains keyword 1 OR keyword 2...
        condition = lit(False)
        for kw in keywords:
            condition = condition | col("text_search").contains(kw)
        
        df = df.withColumn(col_name, when(condition, 1).otherwise(0))

    # --- 5. Compound & Final Select ---
    compounds = ["hyde park", "mountain view", "palm hills", "mivida", "madinaty", "rehab", "eastown", "celia", "badya"]
    compound_expr = when(lit(False), lit(None))
    for cmp in compounds:
        compound_expr = compound_expr.when(col("text_search").contains(cmp), lit(cmp))
    
    df = df.withColumn("compound", compound_expr.otherwise("Unknown"))

    target_cols = [
        "title", "price", "location", "region", "city", "size", "bedrooms", "bathrooms", 
        "compound", "source", "property_type", "link", "description", "price_per_sqm",
        "Jacuzzi", "Garden", "Balcony", "Pool", "Parking", "Gym", "Maids_Quarters", "Spa"
    ]
    
    selected_cols = [c for c in target_cols if c in df.columns]
    df = df.select(*selected_cols)

    output_path = "hdfs://namenode:9000/datalake/gold/bayut_gold"
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Bayut Gold data (Enriched) saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
