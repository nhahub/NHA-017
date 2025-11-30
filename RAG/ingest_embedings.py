# ingest_embeddings_faiss.py
import os
import pandas as pd
import psycopg2
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import pickle

# ----- PostgreSQL Config -----
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "real_state_dwh")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin")

# ----- Connect to PostgreSQL -----
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
print("Connected to PostgreSQL!")

# ----- Fetch Real Estate Data -----
df = pd.read_sql("SELECT * FROM real_estate_one_big_table", conn)
conn.close()
print(f"Loaded {len(df)} properties from DWH.")

# ----- Initialize SentenceTransformer -----
model = SentenceTransformer("all-MiniLM-L6-v2")

# ----- Prepare Texts for Embeddings -----
descriptions = []
for idx, row in df.iterrows():
    amenities = ", ".join([f"{k}={row[k]}" for k in ["jacuzzi","garden","balcony","pool","parking","gym","maids_quarters","spa"]])
    text = (
        f"Title: {row['title']}. Description: {row['description']}. "
        f"Location: {row['location']}, City: {row['city']}, Region: {row['region']}. "
        f"Price: {row['price']} EGP, Price per sqm: {row['price_per_sqm']}. "
        f"Area: {row['area']} sqm, Bedrooms: {row['bedrooms']}, Bathrooms: {row['bathrooms']}. "
        f"Type: {row['property_type']}. Source: {row['source']}. "
        f"Amenities: {amenities}. Latitude: {row['latitude']}, Longitude: {row['longitude']}."
    )
    descriptions.append(text)

# ----- Compute Embeddings -----
embeddings = model.encode(descriptions, show_progress_bar=True)
embeddings = np.array(embeddings).astype("float32")
print("Embeddings computed!")

# ----- Build FAISS Index -----
dimension = embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(embeddings)
print(f"FAISS index created with {index.ntotal} vectors.")

# ----- Save Index and Metadata -----
faiss.write_index(index, "faiss_index.idx")
with open("faiss_metadata.pkl", "wb") as f:
    pickle.dump(df.to_dict(orient="records"), f)
print("FAISS index and metadata saved!")
