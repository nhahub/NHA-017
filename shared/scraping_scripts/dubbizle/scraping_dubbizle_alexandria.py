# dubizzle_full_scraper_tracking_skip_empty.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import signal
import sys

BASE_URL = "https://www.dubizzle.com.eg/en/properties/apartments-duplex-for-sale/alexandria/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
CSV_FILE = "/data/data_csv_files/dubbizle/dubizzle_all_listings_unlimited_pages.csv"
MAX_RETRIES = 2  # retry times if page fails

# Global variable to hold listings for saving on interrupt
all_listings = []

def save_progress():
    """Save the current progress to CSV"""
    if all_listings:
        df = pd.DataFrame(all_listings)
        df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
        print(f"\n💾 Progress saved. Total listings so far: {len(all_listings)}")

def signal_handler(sig, frame):
    print("\n⚠️ Script interrupted! Saving progress...")
    save_progress()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def parse_listing(card):
    try:
        a_tag = card.select_one("div._70cdfb32 a")
        link = "https://www.dubizzle.com.eg" + a_tag["href"] if a_tag else "N/A"
        description = a_tag.get("title", "N/A") if a_tag else "N/A"

        price_span = card.select_one("div[aria-label='Price'] span")
        price = price_span.get_text(strip=True) if price_span else "N/A"

        location_span = card.select_one("span[aria-label='Location']")
        location = location_span.get_text(strip=True) if location_span else "N/A"

        beds_span = card.select_one("span[aria-label='Beds'] ._3e1113f0")
        bedrooms = beds_span.get_text(strip=True) if beds_span else "N/A"

        baths_span = card.select_one("span[aria-label='Bathrooms'] ._3e1113f0")
        bathrooms = baths_span.get_text(strip=True) if baths_span else "N/A"

        area_span = card.select_one("span[aria-label='Area'] ._3e1113f0")
        area = area_span.get_text(strip=True) if area_span else "N/A"

        return {
            "description": description,
            "link": link,
            "price": price,
            "location": location,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "area": area
        }
    except Exception as e:
        print(f"Error parsing listing: {e}")
        return None

def scrape_page(page_number):
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            url = f"{BASE_URL}?page={page_number}&filter=completion_status_eq_1"
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.text, "lxml")

            # Try main selector
            cards = soup.select("div._70cdfb32")
            # Optional: alternative selector if classes change (add more if needed)
            if not cards:
                cards = soup.select("div.listing-card")  # example fallback

            if not cards:
                print(f"⚠️ No listings found on page {page_number}, skipping...")
                return []

            listings = []
            for card in cards:
                data = parse_listing(card)
                if data:
                    listings.append(data)
            return listings

        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Request error on page {page_number}, retry {retries}/{MAX_RETRIES}: {e}")
            time.sleep(2)

    print(f"⚠️ Failed to get page {page_number} after {MAX_RETRIES} retries. Skipping...")
    return []

def main():
    global all_listings
    all_listings = []
    page_number = 1

    while True:
        print(f"📰 Scraping page {page_number}...")
        listings = scrape_page(page_number)
        if listings:
            all_listings.extend(listings)

            # Save after each page
            df = pd.DataFrame(all_listings)
            df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")

            # Print tracking info
            for idx, _ in enumerate(listings, start=1):
                print(f"➡️  Row {len(all_listings)-len(listings)+idx} collected")

            print(f"✅ Page {page_number} scraped successfully.")
            print(f"Total listings collected so far: {len(all_listings)}\n")
        else:
            # Page had no listings (skipped)
            print(f"⏭️ Page {page_number} skipped.")

        page_number += 1
        time.sleep(1)  # polite delay

        # Optional stopping condition: stop if no listings found consecutively
        if page_number > 500:  # safety limit
            print("🚫 Reached page 500, stopping...")
            break

    print(f"🎯 Scraping finished. Total listings collected: {len(all_listings)}")
    print(f"All data saved to {CSV_FILE}")

if __name__ == "__main__":
    main()
