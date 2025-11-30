import psycopg2

def create_table():
    # Connection details for the Data Warehouse (postgres_general)
    db_config = {
        "host": "postgres_general",
        "port": "5432",
        "database": "real_state_dwh",
        "user": "admin",
        "password": "admin"
    }

    ddl_query = """
    DROP TABLE IF EXISTS real_estate_one_big_table;
    CREATE TABLE real_estate_one_big_table (
    listing_id SERIAL PRIMARY KEY,
    title TEXT,
    link TEXT UNIQUE, -- unique natural key
    price DOUBLE PRECISION,
    location TEXT,
    region VARCHAR(255),
    city VARCHAR(255),
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
    description TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX idx_location ON real_estate_one_big_table(city);
    CREATE INDEX idx_price ON real_estate_one_big_table(price);
    CREATE INDEX idx_property_type ON real_estate_one_big_table(property_type);
    CREATE INDEX idx_source ON real_estate_one_big_table(source);
    """

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute(ddl_query)
        conn.commit()
        print("✅ Data Warehouse table 'real_estate_one_big_table' created successfully.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        raise e

if __name__ == "__main__":
    create_table()
