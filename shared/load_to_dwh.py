import sys
import subprocess
import os

# --- 0. Dependency Check ---
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("⚠️ psycopg2 not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

def main():
    print("🚀 Starting ETL Process (Standard + 5th Settlement Fix + Rounding)...")

    spark = SparkSession.builder \
        .appName("Load_To_DWH_Final") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    # --- 1. Read Data ---
    paths = {
        "Bayut": "/datalake/gold/bayut_gold",
        "Dubbizle": "/datalake/gold/dubbizle_gold",
        "Fazwaz": "/datalake/gold/fazwaz_gold",
        "PropertyFinder": "/datalake/gold/propertyfinder_gold"
    }
    
    dataframes = []

    for source_name, path in paths.items():
        print(f"\n📂 Reading {source_name} from {path}...")
        try:
            df = spark.read.parquet(path)
            existing_cols = df.columns
            
            # Fix Link
            link_col = next((c for c in existing_cols if c.lower() in ['link', 'url', 'share_url', 'canonical_url']), None)
            if link_col and link_col != 'link': df = df.withColumnRenamed(link_col, "link")
            elif not link_col: df = df.withColumn("link", lit(None).cast(StringType()))

            # Fix Area
            area_col = next((c for c in existing_cols if c.lower() in ['area', 'size', 'size_sqm', 'area_sqm']), None)
            if area_col and area_col != 'area': df = df.withColumnRenamed(area_col, "area")
            
            # Ensure columns exist
            for c in ["location", "price", "property_type"]:
                if c not in df.columns: df = df.withColumn(c, lit(None))
            
            if "source" not in df.columns: df = df.withColumn("source", lit(source_name))

            dataframes.append((source_name, df))
            
        except Exception as e:
            print(f"❌ Error reading {source_name}: {e}")

    if not dataframes: sys.exit(1)

    # --- 2. APPLY NORMAL TRANSFORMATION LOGIC ---
    processed_dfs = []
    
    for source_name, df in dataframes:
        
        # 2.1 Bayut
        if source_name == "Bayut":
            if "city" in df.columns and "region" in df.columns:
                print("   ✅ Bayut: Using pre-calculated city/region.")
            elif "region" in df.columns:
                df = df.withColumnRenamed("region", "city").withColumn("region", col("location"))

        # 2.2 Dubbizle
        elif source_name == "Dubbizle":
            df = df.withColumn("clean_loc", regexp_replace(col("location"), "[.•]+$", ""))
            df = df.withColumn(
                "city",
                when(col("source").contains("Cairo") | lower(col("location")).contains("cairo"), "Cairo")
                .otherwise("Alexandria")
            )
            df = df.withColumn("split_arr", split(col("clean_loc"), ","))
            last_token = trim(element_at(col("split_arr"), -1))
            df = df.withColumn(
                "region",
                when(lower(last_token).isin("cairo", "alexandria", "alex", "egypt"), 
                     trim(element_at(col("split_arr"), -2)))
                .otherwise(last_token)
            ).drop("split_arr", "clean_loc")

        # 2.3 Fazwaz
        elif source_name == "Fazwaz":
            df = df.withColumn("location_cleaned", regexp_replace(col("location"), ", Egypt$", ""))
            df = df.withColumn("split_arr", split(col("location_cleaned"), ","))
            df = df.withColumn("split_size", size(col("split_arr")))
            df = df.withColumn("city", trim(element_at(col("split_arr"), -1)))
            df = df.withColumn("temp_region", trim(element_at(col("split_arr"), -2)))
            df = df.withColumn(
                "region",
                when(
                    (col("temp_region").isin("New Cairo", "New Cairo City")) & (col("split_size") >= 3),
                    trim(element_at(col("split_arr"), -3))
                ).otherwise(col("temp_region"))
            ).drop("location_cleaned", "split_arr", "split_size", "temp_region")

        # 2.4 PropertyFinder
        elif source_name == "PropertyFinder":
            df = df.withColumn("location_lower", lower(col("location")))
            df = df.withColumn(
                "city",
                when(col("location_lower").rlike("alex|alexandria|alex governorate|elx"), "Alexandria")
                .when(col("location_lower").rlike("cairo|giza|nasr city|heliopolis|maadi|rehab|tagamo3|new cairo|madinaty|badr|shorouk|obour|sheikh zayed|zayed|6th october|6 october|october|hadayek october|october gardens|new giza|palm hills|gardenia|zayed dunes|dahshour|الشيخ زايد|٦ أكتوبر|اكتوبر|حدائق اكتوبر"), "Cairo")
                .when(col("location_lower").rlike("north coast|sidi abdel rahman|al alamein|ras al hekm|dabaa"), "North Coast")
                .when(col("location_lower").rlike("ain sokhna|sokhna|suez|galala"), "Sokhna")
                .when(col("location_lower").rlike("hurghada|red sea"), "Red Sea")
                .otherwise("Other")
            )
            df = df.withColumn("split_pf", split(col("location"), ","))
            df = df.withColumn("split_size", size(col("split_pf")))
            df = df.withColumn("temp_region", trim(element_at(col("split_pf"), -2)))
            df = df.withColumn(
                "region", 
                when(
                    (col("temp_region").isin("New Cairo City", "New Cairo")) & (col("split_size") >= 3),
                    trim(element_at(col("split_pf"), -3))
                ).otherwise(col("temp_region"))
            ).drop("location_lower", "split_pf", "split_size", "temp_region")

        processed_dfs.append(df)

    # --- 3. Merge ---
    print("\n🔄 Merging datasets...")
    final_columns = [
        "title", "link", "price", "location", "region", "city", "area", "bedrooms", "bathrooms",
        "latitude", "longitude", "property_type", "source", "price_per_sqm",
        "Jacuzzi", "Garden", "Balcony", "Pool", "Parking", "Gym",
        "Maids_Quarters", "Spa", "description"
    ]

    def safe_select(df, columns):
        existing_cols = df.columns
        df = df.dropDuplicates()
        select_expr = [col(c) if c in existing_cols else lit(None).cast(StringType()).alias(c) for c in columns]
        return df.select(select_expr)

    df_all = safe_select(processed_dfs[0], final_columns)
    for i in range(1, len(processed_dfs)):
        df_all = df_all.unionByName(safe_select(processed_dfs[i], final_columns))
    
    # --- 4. CLEANING & 5TH SETTLEMENT FIX ---
    print("🧹 Cleaning & Applying 5th Settlement Fix...")
    
    # Standard Polish
    df_all = df_all.withColumn("property_type", initcap(trim(col("property_type"))))
    df_all = df_all.withColumn("city", initcap(regexp_replace(trim(col("city")), "[.•]$", "")))
    df_all = df_all.withColumn("region", initcap(regexp_replace(trim(col("region")), "[.•]$", "")))
    
    # --- THE 5TH SETTLEMENT EXCEPTION ---
    df_all = df_all.withColumn(
        "region",
        when(lower(col("location")).contains("5th settlement"), "5th Settlement")
        .otherwise(col("region"))
    )

    # Map Variations
    df_all = df_all.withColumn("city", when(col("city") == "Alex", "Alexandria").otherwise(col("city")))
    
    # Clean up other specific region names
    df_all = df_all.withColumn("region", regexp_replace(col("region"), "The 5th Settlement", "5th Settlement"))
    df_all = df_all.withColumn("region", regexp_replace(col("region"), "New Cairo City", "New Cairo"))
    
    # Prevent City == Region
    df_all = df_all.withColumn("region", when(col("region") == col("city"), None).otherwise(col("region")))

    # --- 5. Filters & Casts (WITH ROUNDING) ---
    df_all = df_all.withColumn("price", col("price").cast(DoubleType()))
    df_all = df_all.withColumn("area", col("area").cast(DoubleType()))
    
    # --- ROUNDING PRICE PER SQM HERE ---
    df_all = df_all.withColumn("price_per_sqm", round(col("price_per_sqm").cast(DoubleType()), 2))
    
    df_all = df_all.filter(col("link").isNotNull())
    df_all = df_all.dropDuplicates(["link"]) 
    df_all = df_all.filter(col("price") <= 1000000000)
    
    df_all = df_all.withColumn("bedrooms", col("bedrooms").cast(IntegerType()))
    df_all = df_all.withColumn("bathrooms", col("bathrooms").cast(IntegerType()))
    df_all = df_all.filter((col("bedrooms") < 100) & (col("bathrooms") < 100))
    
    final_count = df_all.count()
    print(f"📉 Final Rows to Insert: {final_count}")
    if final_count == 0: sys.exit(1)

    # --- 6. Write to Postgres ---
    print("🐼 Converting to Pandas...")
    pdf = df_all.toPandas()
    
    conn_params = {
        "dbname": "real_state_dwh",
        "user": "admin",
        "password": "admin",
        "host": "postgres_general",
        "port": 5432
    }

    create_table_sql = """
    DROP TABLE IF EXISTS real_estate_one_big_table;
    CREATE TABLE real_estate_one_big_table (
        title TEXT,
        link TEXT PRIMARY KEY,
        price NUMERIC, 
        location TEXT,
        region TEXT,
        city TEXT,
        area NUMERIC,
        bedrooms INTEGER,
        bathrooms INTEGER,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        property_type TEXT,
        source TEXT,
        price_per_sqm NUMERIC,
        "jacuzzi" INTEGER,
        "garden" INTEGER,
        "balcony" INTEGER,
        "pool" INTEGER,
        "parking" INTEGER,
        "gym" INTEGER,
        "maids_quarters" INTEGER,
        "spa" INTEGER,
        description TEXT,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    insert_sql = """
    INSERT INTO real_estate_one_big_table (
        title, link, price, location, region, city, area, bedrooms, bathrooms, latitude, longitude,
        property_type, source, price_per_sqm, "jacuzzi", "garden", "balcony", "pool",
        "parking", "gym", "maids_quarters", "spa", description
    ) VALUES %s
    ON CONFLICT (link) DO NOTHING;
    """

    conn = None
    cur = None
    try:
        print("🔌 Connecting to PostgreSQL...")
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        print("🏗️  Recreating Table...")
        cur.execute(create_table_sql)

        print("💾 Inserting data...")
        pdf = pdf.where(pd.notnull(pdf), None)
        data_tuples = [tuple(x) for x in pdf.to_numpy()]
        execute_values(cur, insert_sql, data_tuples)
        
        conn.commit()
        print("✅ Data loaded successfully!")

    except Exception as e:
        print(f"❌ Error during Database Operation: {e}")
        if conn: conn.rollback()
        raise e
    finally:
        if cur: cur.close()
        if conn: conn.close()
        spark.stop()

if __name__ == "__main__":
    main()
