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

--- Create analytics Data mart


CREATE SCHEMA IF NOT EXISTS analytics_dm;

CREATE TABLE IF NOT EXISTS analytics_dm.real_estate_analytics (
    analytics_id SERIAL PRIMARY KEY,
    dwh_listing_id INTEGER, -- FK reference ID back to DWH table
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

-- ✅ Create indexes on analytics table
CREATE INDEX idx_city_dm ON analytics_dm.real_estate_analytics(city);
CREATE INDEX idx_price_dm ON analytics_dm.real_estate_analytics(price);
CREATE INDEX idx_property_type_dm ON analytics_dm.real_estate_analytics(property_type);
CREATE INDEX idx_source_dm ON analytics_dm.real_estate_analytics(source);



-- Create the data mart table for the ml model by selecting the features for the model
CREATE TABLE ml_data_mart AS
SELECT
    -- Target Variable
    price,

    -- Numerical Features
    area,
    bedrooms,
    bathrooms,
    latitude,
    longitude,
    price_per_sqm,
    "jacuzzi",
    "garden",
    "balcony",
    "pool",
    "parking",
    "gym",
    "maids_quarters",
    "spa",

    -- Categorical Features (CRITICAL for a good model)
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
