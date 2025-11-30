from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import time
import sys
import signal

# -----------------------------------
# Features to extract
# -----------------------------------
FEATURES_LIST = [
    "Private Pool",
    "Private Gym",
    "Private Garden",
    "Covered Parking",
    "Maids Quarters",
    "Jacuzzi",
    "Garden",
    "Balcony"
]

# -----------------------------------
# Global data storage
# -----------------------------------
all_data = []

# -----------------------------------
# Save collected data to CSV
# -----------------------------------
def save_data(filename="fazwaz_apartments_partial.csv"):
    if all_data:
        df = pd.DataFrame(all_data)
        df.fillna("N/A", inplace=True)
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"\n💾 Saved {len(df)} rows to {filename}")

# -----------------------------------
# Handle Ctrl+C gracefully
# -----------------------------------
def signal_handler(sig, frame):
    print("\n⏹️ Scraping interrupted by user!")
    save_data()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# -----------------------------------
# Extract apartment details from a listing page
# -----------------------------------
def extract_apartment_details(page):
    soup = BeautifulSoup(page.content(), "lxml")

    def safe_select(selector):
        element = soup.select_one(selector)
        return element.get_text(strip=True) if element else "N/A"

    try:
        data = {
            "unit_id": safe_select("strong.unit-info-element__item__unit-id"),
            "name": safe_select("h1.unit-name"),
            "price": safe_select("div.price-message"),
            "about": safe_select("div.unit-view-description"),
            "bedrooms": "N/A",
            "bathrooms": "N/A",
            "size": "N/A",
            "property_type": "N/A",
            "location": safe_select("div.clear_flex span.project-location"),
            "price_per_sqm": "N/A",
            "link": page.url,
            **{f: "N/A" for f in FEATURES_LIST}
        }

        # Property info
        for elem in soup.select("div.property-info-element"):
            text = elem.get_text(" ", strip=True)
            if "Bedrooms" in text:
                data["bedrooms"] = text.split(" ")[0]
            elif "Bathrooms" in text:
                data["bathrooms"] = text.split(" ")[0]
            elif "SqM" in text:
                data["size"] = text.split(" ")[0]

        # Basic information
        for item in soup.select("div.basic-information__item"):
            topic = item.select_one(".basic-information-topic")
            info = item.select_one(".basic-information-info")
            if topic and info:
                key = topic.get_text(strip=True).lower().replace(" ", "_")
                value = info.get_text(strip=True)
                if "property_type" in key:
                    data["property_type"] = value
                elif "location" in key:
                    data["location"] = value
                elif "price_per_sqm" in key:
                    data["price_per_sqm"] = value

        # Features
        feature_elements = [f.get_text(strip=True) for f in soup.select("div.unit-features span.unit-features__item")]
        for feature in FEATURES_LIST:
            if any(feature.lower() in f.lower() for f in feature_elements):
                data[feature] = "mawgood"

    except Exception as e:
        print(f"⚠️ Error extracting data from {page.url}: {e}")
        return None

    return data

# -----------------------------------
# Extract all listing links from a page
# -----------------------------------
def extract_listing_links(soup):
    links = []
    for a in soup.select("a.link-unit"):
        href = a.get("href")
        if href and href.startswith("https"):
            links.append(href)
    return links

# -----------------------------------
# Safe page navigation with retries
# -----------------------------------
def safe_goto(page, url, retries=3):
    for attempt in range(retries):
        try:
            page.goto(url, wait_until="load", timeout=60000)
            page.wait_for_selector("h1.unit-name", timeout=30000)
            return True
        except Exception as e:
            print(f"⚠️ Retry {attempt+1} failed for {url}: {e}")
            time.sleep(3)
    return False

# -----------------------------------
# Main scraping function
# -----------------------------------
def main():
    base_urls = [ "https://www.fazwaz.com.eg/en/property-for-sale/egypt/cairo/nasr-city",
        "https://www.fazwaz.com.eg/en/property-for-sale/egypt/cairo",
        "https://www.fazwaz.com.eg/en/property-for-sale/egypt/cairo/new-capital-city"
       
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for idx_base_url, base_url in enumerate(base_urls):
            print(f"\n📍 Starting base URL {idx_base_url}: {base_url}")
            page_num = 1

            while True:
                url = f"{base_url}?page={page_num}"
                try:
                    print(f"\n🌐 Scraping page {page_num} | URL: {url} | Total rows: {len(all_data)}")
                    page.goto(url, wait_until="load", timeout=60000)
                    time.sleep(2)  # slight delay to avoid rate-limiting

                    soup = BeautifulSoup(page.content(), "lxml")
                    listing_links = extract_listing_links(soup)

                    if not listing_links:
                        print("🚫 No more listings on this base URL, moving to next.")
                        break

                    for idx, link in enumerate(listing_links, start=1):
                        try:
                            print(f"→ Scraping listing {idx}/{len(listing_links)} | Total rows: {len(all_data)}")
                            if safe_goto(page, link):
                                data = extract_apartment_details(page)
                                if data:
                                    all_data.append(data)
                            else:
                                print(f"⚠️ Skipping listing after repeated failures: {link}")
                        except Exception as e:
                            print(f"⚠️ Error scraping listing {link}: {e}")
                            save_data()

                except Exception as e:
                    print(f"⚠️ Error loading page {url}: {e}")
                    save_data()
                    break  # move to next base URL

                page_num += 1

        browser.close()

    # Final save
    save_data("/data/data_csv_files/fazwaz/fazwaz_apartments_allcombined.csv")
    print(f"\n✅ Scraping complete. Total rows collected: {len(all_data)}")

# -----------------------------------
# Entry point
# -----------------------------------
if __name__ == "__main__":
    main()
