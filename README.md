# DEPI-Final-Project

5. **Check PostgreSQL**
   - Open **pgAdmin** at [http://localhost:8085](http://localhost:8085)  
   - Login with:  
     - Email: `admin@admin.com`  
     - Password: `admin`  

6. **Create PostgreSQL Connection**
   - In pgAdmin, create a new server connection:
     - **Name**: `postgres_general`  
     - **Host**: `postgres_general`  
     - **Port**: `5432`  
     - **Username**: `admin`  
     - **Password**: `admin`  

7. **Create Database**
   - Open **Query Tool** in pgAdmin and run:
     ```sql
     CREATE DATABASE real_state_dwh;
     ```
!['Create one big table table']


8. **Create  Table**
  CREATE TABLE real_estate_one_big_table (
    listing_id SERIAL PRIMARY KEY,
    title TEXT,
    link TEXT,
    price DOUBLE PRECISION,
    location TEXT,
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
    description TEXT
);


CREATE INDEX idx_location ON real_estate_one_big_table(location);
CREATE INDEX idx_price ON real_estate_one_big_table(price);
CREATE INDEX idx_property_type ON real_estate_one_big_table(property_type);
CREATE INDEX idx_source ON real_estate_one_big_table(source);
     ```

9. **Run Spark Script**
   - Open the `scripts` folder and copy the Spark script into a Jupyter Notebook.  
   - Access Jupyter at [http://localhost:8888](http://localhost:8888).  
   - Before running the script, install PostgreSQL driver:
     ```bash
     pip install psycopg2-binary
     ```
   - Run the notebook and confirm that records are being inserted into PostgreSQL.