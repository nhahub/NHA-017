from pyspark.sql import SparkSession
import os

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("IngestToBronze") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    # Mapping: Local CSV path -> Target Parquet Filename
    files_mapping = {
        "/data/data_csv_files/bayut/bayut_final.csv": "bayutData",
        "/data/data_csv_files/dubbizle/dubizzle_all_listings_unlimited_pages.csv": "dubbizle_alexandria",
        "/data/data_csv_files/dubbizle/dubizzle_all_listings_cairo.csv": "dubizzle_all_listings_cairo",
        "/data/data_csv_files/fazwaz/fazwaz_apartments_allcombined.csv": "fazwaz_apartments_allcombined",
        "/data/data_csv_files/propertyfinder/propertyfinder.csv": "propertyfinder"
    }

    # Target HDFS Path (Where we want to save)
    hdfs_bronze_path = "/datalake/bronze/"

    for local_csv, parquet_name in files_mapping.items():
        print(f"\nProcessing {local_csv}...")
        
        # 1. Check if file exists on the Container's Local Disk
        if not os.path.exists(local_csv):
            print(f"⚠️ File not found: {local_csv}. It might have been skipped by the scraper.")
            continue

        try:
            # 2. READ: Use 'file://' to force Spark to read from the Container's Local Disk
            # Without this, it looks in HDFS and fails.
            local_path_with_protocol = f"file://{local_csv}"
            
            df = spark.read.option("header", True).csv(local_path_with_protocol)
            
            # 3. WRITE: Save to HDFS (Spark defaults to HDFS for writing because of config above)
            target_path = f"{hdfs_bronze_path}{parquet_name}"
            df.write.mode("overwrite").parquet(target_path)
            
            print(f"✅ Saved to hdfs://namenode:9000{target_path}")
            
        except Exception as e:
            print(f"❌ Error processing {local_csv}: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
