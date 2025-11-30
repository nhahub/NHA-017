import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time

base_url = "https://www.propertyfinder.eg/en/buy/properties-for-sale.html"
headers = {"User-Agent": "Mozilla/5.0"}

all_results = []

for page in range(1, 85):
    url = f"{base_url}?page={page}"
    print(f"Scraping page {page} ...")

    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    data = json.loads(script_tag.string)

    listings = data["props"]["pageProps"]["searchResult"]["listings"]

    for item in listings:
        prop = item["property"]

        bathrooms = prop.get("bathrooms") or prop.get("property_unit", {}).get("bathrooms")

        geo = prop.get("location", {}).get("geographical_coordinates", {})
        lat = geo.get("latitude")
        lng = geo.get("longitude")

        all_results.append({
            "id": prop.get("id"),
            "title": prop.get("title"),
            "price": prop["price"]["value"] if prop.get("price") else None,
            "currency": prop["price"]["currency"] if prop.get("price") else None,
            "location": prop.get("location", {}).get("full_name"),
            "bedrooms": prop.get("bedrooms"),
            "bathrooms": bathrooms,
            "property_type": prop.get("property_type"),
            "property_size": f'{prop["size"]["value"]} {prop["size"]["unit"]}' if prop.get("size") else None,
            "furnished": prop.get("furnished"),
            "share_url": prop.get("share_url"),
            "description": prop.get("description"),
            "amenities": ", ".join(prop.get("amenity_names", [])),
            "listed_date": prop.get("listed_date"),
            "latitude": prop["location"]["coordinates"]["lat"] if prop.get("location") else None,
            "longitude": prop["location"]["coordinates"]["lon"] if prop.get("location") else None
        })

    time.sleep(1)



df = pd.DataFrame(all_results)
import re

def clean_description(text, max_len=250):
    if not isinstance(text, str):
        return ""

    # Remove Arabic letters
    text = re.sub(r'[\u0600-\u06FF]+', ' ', text)

    # Remove phone numbers
    text = re.sub(r'\+?\d[\d\s-]{7,}', ' ', text)

    # Remove HTML entities like &amp;
    text = re.sub(r'&[A-Za-z]+;', ' ', text)

    # Remove long dashes or decoration lines
    text = re.sub(r'-{2,}', ' ', text)

    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # Keep only first max_len characters
    return text[:max_len]

# Apply cleaning to your column
df["description"] = df["description"].apply(clean_description)

print(df.shape)
df.to_csv("/data/data_csv_files/propertyfinder/propertyfinder.csv", index=False, encoding="utf-8-sig")

print(json.dumps(prop, indent=2))
