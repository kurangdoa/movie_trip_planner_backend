import urllib.request
import duckdb
import os
import json
import cloudscraper
from urllib.parse import quote, urlparse, urlunparse

scraper = cloudscraper.create_scraper()

def safe_download(url):
    # Split the URL to protect the 'https://' part and only encode the path
    p = urlparse(url)
    # Quote the path but allow slashes
    safe_path = quote(p.path, safe='/')
    # Rebuild the URL
    safe_url = urlunparse((p.scheme, p.netloc, safe_path, p.params, p.query, p.fragment))
    
    print(f"🔗 Requesting safe URL: {safe_url}")
    return scraper.get(safe_url, stream=True)

# Define and Create your Data Directory
DATA_DIR = "_duckdb-data"
os.makedirs(DATA_DIR, exist_ok=True)

# Read from json
SOURCE_PATH = "_temp/insideairbnb.json"
with open(SOURCE_PATH, 'r', encoding='utf-8') as file:
    source = json.load(file)

# # --- Configuration ---
# urls_listing = ["https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-09-11/data/listings.csv.gz"
#                , "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz"
#                 ]
# urls_review = ["https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-09-11/data/reviews.csv.gz"
#               , "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/reviews.csv.gz"
#                 ]

def ingestion_airbnb(city: str, province: str, country: str, url_listing: str, db: duckdb.DuckDBPyConnection):
    table_name_listing = "airbnb_listing_raw"
    local_listing_file = os.path.join(DATA_DIR, f"temp_{city}_listings.csv.gz")

    print(f"📍 Target: {city} | {province} | {country}")
    print(f"📥 Downloading listings to {local_listing_file}...")
    urllib.request.urlretrieve(url_listing, local_listing_file)

    print("🚀 Ingesting local files into DuckDB...")

    # 1. Create the base table ONLY if it doesn't exist yet (Strict 12-column schema)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name_listing} (
            id BIGINT,
            name VARCHAR,
            description VARCHAR,
            neighborhood_overview VARCHAR,
            price VARCHAR,
            property_type VARCHAR,
            listing_url VARCHAR,
            picture_url VARCHAR,
            number_of_reviews INT,
            city VARCHAR,
            province VARCHAR,
            country VARCHAR
        );
    """)

    # 2. Delete existing data for this city to prevent duplicates
    db.execute(f"DELETE FROM {table_name_listing} WHERE city = '{city}';")

    # 3. Read the CSV and INSERT directly into the table (No temp tables needed!)
    db.execute(f"""
        INSERT INTO {table_name_listing}
        SELECT
            CAST(id AS BIGINT) as id,
            CAST(name AS VARCHAR) as name, 
            CAST(description AS VARCHAR) as description, 
            CAST(neighborhood_overview AS VARCHAR) as neighborhood_overview,
            CAST(price AS VARCHAR) as price,
            CAST(property_type AS VARCHAR) as property_type, 
            CAST(listing_url AS VARCHAR) as listing_url,
            CAST(picture_url AS VARCHAR) as picture_url,
            CAST(number_of_reviews AS INT) AS number_of_reviews,
            '{city}' AS city,
            '{province}' AS province,
            '{country}' AS country
        FROM read_csv_auto('{local_listing_file}', all_varchar=false);
    """)

    # Verification
    count_listing = db.execute(f"SELECT count(*) FROM {table_name_listing}").fetchone()[0]
    print(f"✅ Success! DuckDB now holds {count_listing} total rows in {table_name_listing}!")

    # Cleanup
    if os.path.exists(local_listing_file):
        os.remove(local_listing_file)

# ==========================================
# TRIGGER THE PIPELINE
# ==========================================

# 1. Open ONE connection for the entire loop
db_path = os.path.join(DATA_DIR, "hotel_movie.db")
db = duckdb.connect(db_path)

# (Optional) Drop the bloated table so we start fresh with the new schema!
# db.execute("DROP TABLE IF EXISTS airbnb_listing_raw;")

for s in source:
    city = s['city']
    province = s['province']
    country = s['country']
    url_listing = s['listing_url']
    
    # 2. Pass the single open connection to the function
    ingestion_airbnb(city, province, country, url_listing, db)

# 3. Compress the file and close the connection safely
print("🗜️ Shrinking database file...")
db.execute("CHECKPOINT;")
db.execute("VACUUM;") 
db.close()

print("🎉 Pipeline Complete! Check your _duckdb-data folder.")