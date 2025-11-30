import psycopg2

def populate_datamarts():
    db_config = {
        "host": "postgres_general",
        "port": "5432",
        "database": "real_state_dwh",
        "user": "admin",
        "password": "admin"
    }

    # 1. Populate Analytics Table
    # Simple INSERT INTO ... SELECT from the main DWH table
    analytics_insert = """
    INSERT INTO analytics_dm.real_estate_analytics (
        link, price, location, region, city, area, bedrooms, bathrooms, 
        latitude, longitude, property_type, source, price_per_sqm, 
        Jacuzzi, Garden, Balcony, Pool, Parking, Gym, Maids_Quarters, Spa
    )
    SELECT 
        link, price, location, region, city, area, bedrooms, bathrooms, 
        latitude, longitude, property_type, source, price_per_sqm, 
        Jacuzzi, Garden, Balcony, Pool, Parking, Gym, Maids_Quarters, Spa
    FROM real_estate_one_big_table;
    """

    # 2. Create & Populate ML Data Mart
    # Logic: "CREATE TABLE AS SELECT" automatically creates the table structure based on the query results.
    ml_create_as = """
    CREATE TABLE ml_data_mart AS
    SELECT
        price,
        area,
        bedrooms,
        bathrooms,
        latitude,
        longitude,
        price_per_sqm,
        "jacuzzi" AS jacuzzi,
        "garden" AS garden,
        "balcony" AS balcony,
        "pool" AS pool,
        "parking" AS parking,
        "gym" AS gym,
        "maids_quarters" AS maids_quarters,
        "spa" AS spa,
        city,
        region,
        property_type,
        title,
        description,
        link
    FROM
        real_estate_one_big_table
    WHERE
        price IS NOT NULL
        AND area IS NOT NULL
        AND bedrooms IS NOT NULL
        AND bathrooms IS NOT NULL
        AND city IS NOT NULL;
    """

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        print("Populating Analytics Data Mart...")
        cur.execute(analytics_insert)
        print(f"✅ Inserted {cur.rowcount} rows into analytics_dm.real_estate_analytics.")

        print("Creating & Populating ML Data Mart...")
        cur.execute(ml_create_as)
        print(f"✅ Created ml_data_mart with {cur.rowcount} rows.")

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error populating data marts: {e}")
        raise e

if __name__ == "__main__":
    populate_datamarts()
