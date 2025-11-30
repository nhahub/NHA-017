import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time
import random
import re

# Note: You may need to install cloudscraper: pip install cloudscraper
try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    print("Warning: 'cloudscraper' library not found. Falling back to 'requests'.")

session = cloudscraper.create_scraper() if HAS_CLOUDSCRAPER else requests.Session()

def get_response(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1"
    }
    try:
        return session.get(url, headers=headers, timeout=30)
    except Exception as e:
        print(f"    ⚠ Network Error: {e}")
        return None

def scrape_property_details(url):
    details = {}
    response = get_response(url)
    
    if not response or response.status_code != 200:
        return {}

    soup = BeautifulSoup(response.text, "html.parser")

    try:
        # 1. Title
        title = soup.find("h1")
        if title: details["title"] = title.get_text(strip=True)

        # 2. Price & Currency
        # Find price by aria-label or class
        price_tag = soup.find(attrs={"aria-label": "Price"})
        if not price_tag:
             price_tag = soup.find(["span", "div"], class_=lambda x: x and "price" in str(x).lower())
        
        if price_tag:
            raw_price = price_tag.get_text(strip=True)
            # Extract only digits for price
            digits = re.sub(r'[^\d]', '', raw_price)
            if digits:
                details["price"] = digits
                
            # FORCE CURRENCY Logic
            if "AED" in raw_price:
                details["currency"] = "AED"
            elif "USD" in raw_price or "$" in raw_price:
                details["currency"] = "USD"
            else:
                # Default to EGP for bayut.eg if logic fails
                details["currency"] = "EGP"
        
        # 3. Area (SQM) - IMPROVED LOGIC
        # Strategy A: Look for aria-label "Area" (Most reliable)
        area_tag = soup.find(attrs={"aria-label": "Area"})
        if area_tag:
             # Text is likely "160 Sq. M."
             raw_area = area_tag.get_text(strip=True)
             area_digits = re.search(r'([\d,]+)', raw_area)
             if area_digits:
                 details["area_sqm"] = area_digits.group(1).replace(",", "")
        
        # Strategy B: Look for generic text patterns if Strategy A failed
        if "area_sqm" not in details:
            text_blob = soup.get_text(" ", strip=True)
            # Match "160 Sq. M.", "160 Sq.M", "160 m2", "160 sqm"
            area_match = re.search(r'([\d,]+)\s*(?:Sq\.\s*M\.?|m²|sqm|sqft)', text_blob, re.IGNORECASE)
            if area_match:
                details["area_sqm"] = area_match.group(1).replace(",", "")

        # 4. Location
        loc_tag = soup.find(attrs={"aria-label": "Location"})
        if loc_tag:
             details["location"] = loc_tag.get_text(strip=True)
        else:
            crumbs = soup.select("div[aria-label='Breadcrumb'] a")
            if crumbs:
                details["location"] = ", ".join([c.get_text(strip=True) for c in crumbs[-3:]])

        # 5. Beds / Baths
        text_blob = soup.get_text(" ", strip=True)
        if "bedrooms" not in details:
            bed_match = re.search(r'(\d+)\s*(?:Bed|Bedroom)', text_blob, re.IGNORECASE)
            if bed_match: details["bedrooms"] = bed_match.group(1)
            
        if "bathrooms" not in details:
            bath_match = re.search(r'(\d+)\s*(?:Bath|Bathroom)', text_blob, re.IGNORECASE)
            if bath_match: details["bathrooms"] = bath_match.group(1)

    except Exception as e:
        print(f"      ⚠ HTML Parse Error: {e}")

    # Log for debugging
    p = details.get('price', 'N/A')
    c = details.get('currency', 'N/A')
    a = details.get('area_sqm', 'N/A')
    print(f"      ✓ Scraped: Price:{p} {c} | Area:{a}")
    
    return details

def extract_listing_urls(soup):
    urls = set()
    
    # 1. Standard Article tags
    articles = soup.find_all("article")
    for article in articles:
        link = article.find("a", href=True)
        if link: urls.add(link['href'])

    # 2. List Items (aria-label="Listing")
    if not urls:
        listings = soup.find_all("li", attrs={"aria-label": "Listing"})
        for item in listings:
            link = item.find("a", href=True)
            if link: urls.add(link['href'])
            
    full_urls = []
    for u in urls:
        if u.startswith("/"):
            full_urls.append(f"https://www.bayut.eg{u}")
        elif u.startswith("http"):
            full_urls.append(u)
            
    print(f"    → Found {len(full_urls)} listing URLs.")
    return list(full_urls)

def scrape_main():
    # URL pattern
    BASE_URL = "https://www.bayut.eg/en/egypt/properties-for-sale/page-{}/?furnishing_status=furnished"
    
    all_records = []
    
    # Run for 2 pages
    for page in range(1, 101):
        if page == 1:
            url = "https://www.bayut.eg/en/egypt/properties-for-sale/?furnishing_status=furnished"
        else:
            url = BASE_URL.format(page)
            
        print(f"\n--- Page {page} ---")
        response = get_response(url)
        
        if not response or response.status_code != 200:
            print("❌ Blocked or Failed.")
            continue
            
        soup = BeautifulSoup(response.text, "html.parser")
        urls = extract_listing_urls(soup)
        
        for i, prop_url in enumerate(urls):
            print(f"  [{i+1}/{len(urls)}] {prop_url}")
            data = scrape_property_details(prop_url)
            if data:
                data['url'] = prop_url
                all_records.append(data)
            time.sleep(random.uniform(1.5, 3))

    if not all_records:
        print("❌ No data collected.")
        return

    df = pd.DataFrame(all_records)
    
    # Final cleanup
    if "price" in df.columns:
        df = df.dropna(subset=["price"])
    
    filename = "/data/data_csv_files/bayut/bayut_final.csv"
    df.to_csv(filename, index=False)
    print(f"\n✔ Saved {len(df)} rows to {filename}")
    print(df[['price', 'currency', 'area_sqm']].head())

if __name__ == "__main__":
    scrape_main()
