import psycopg2

def create_datamarts():
    db_config = {
        "host": "postgres_general",
        "port": "5432",
        "database": "real_state_dwh",
        "user": "admin",
        "password": "admin"
    }

    # 1. Analytics Data Mart DDL
    analytics_ddl = """
    CREATE SCHEMA IF NOT EXISTS analytics_dm;

    DROP TABLE IF EXISTS analytics_dm.real_estate_analytics;

    CREATE TABLE analytics_dm.real_estate_analytics (
        analytics_id SERIAL PRIMARY KEY,
        -- We don't have a 'dwh_listing_id' column in the main table yet, 
        -- so we'll skip the FK constraint for now or use the link as a reference.
        link TEXT, 
        price DOUBLE PRECISION,
        location TEXT,
        region TEXT,
        city TEXT,
        area DOUBLE PRECISION,
        bedrooms INTEGER,
        bathrooms INTEGER,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        property_type TEXT,
        source TEXT,
        price_per_sqm DOUBLE PRECISION,
        Jacuzzi INTEGER,
        Garden INTEGER,
        Balcony INTEGER,
        Pool INTEGER,
        Parking INTEGER,
        Gym INTEGER,
        Maids_Quarters INTEGER,
        Spa INTEGER,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_city_dm ON analytics_dm.real_estate_analytics(city);
    CREATE INDEX IF NOT EXISTS idx_price_dm ON analytics_dm.real_estate_analytics(price);
    CREATE INDEX IF NOT EXISTS idx_property_type_dm ON analytics_dm.real_estate_analytics(property_type);
    CREATE INDEX IF NOT EXISTS idx_source_dm ON analytics_dm.real_estate_analytics(source);
    """

    # 2. ML Data Mart DDL
    # Note: 'CREATE TABLE AS SELECT' creates the table and populates it in one go.
    # But to separate DDL vs DML, we will just DROP it here to ensure a fresh start.
    ml_ddl_drop = "DROP TABLE IF EXISTS ml_data_mart;"

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # Run Analytics DDL
        cur.execute(analytics_ddl)
        print("✅ Analytics Data Mart table created.")

        # Run ML Drop (We will recreate it in the population step)
        cur.execute(ml_ddl_drop)
        print("✅ ML Data Mart table dropped (ready for recreation).")

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creating data marts: {e}")
        raise e

if __name__ == "__main__":
    create_datamarts()
