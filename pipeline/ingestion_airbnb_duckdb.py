import urllib.request
import duckdb
import os

# 📁 1. Define and Create your Data Directory
DATA_DIR = "_duckdb-data"
os.makedirs(DATA_DIR, exist_ok=True) # Creates the folder safely!

# --- Configuration ---
url_listing = "http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2023-12-12/data/listings.csv.gz"
url_review = "http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2023-12-12/data/reviews.csv.gz"

table_name_listing = "airbnb_listing_raw"
table_name_review = "airbnb_review_raw"

# 📁 2. Point all files inside the new directory
local_listing_file = os.path.join(DATA_DIR, "temp_listings.csv.gz")
local_review_file = os.path.join(DATA_DIR, "temp_reviews.csv.gz")
db_path = os.path.join(DATA_DIR, "hotel_movie.db")

# --- Extract Metadata ---
city_listing = url_listing.split("/")[-4]
province_listing = url_listing.split("/")[-5]
country_listing = url_listing.split("/")[-6]

city_review = url_review.split("/")[-4]
province_review = url_review.split("/")[-5]
country_review = url_review.split("/")[-6]

print(f"📍 Target: {city_listing} | {province_listing} | {country_listing}")

# --- 1. Download Phase ---
print(f"📥 Downloading listings to {local_listing_file}...")
urllib.request.urlretrieve(url_listing, local_listing_file)

print(f"📥 Downloading reviews to {local_review_file}...")
urllib.request.urlretrieve(url_review, local_review_file)

# --- 2. Database Phase ---
print(f"🔌 Connecting to DuckDB at {db_path}...")
# 🚀 FIX: Connect to the DB inside the folder
db = duckdb.connect(db_path) 

print("🚀 Ingesting local files into DuckDB...")

# Create the listings table
query_listing = f"""
    CREATE OR REPLACE TABLE {table_name_listing} AS 
    SELECT
        *, 
        '{city_listing}' AS city, 
        '{province_listing}' AS province, 
        '{country_listing}' AS country 
    FROM read_csv_auto('{local_listing_file}', all_varchar=true)
"""
db.execute(query_listing)

# Create the reviews table
query_review = f"""
    CREATE OR REPLACE TABLE {table_name_review} AS 
    SELECT 
        *, 
        '{city_review}' AS city, 
        '{province_review}' AS province, 
        '{country_review}' AS country 
    FROM read_csv_auto('{local_review_file}', all_varchar=true)
"""
db.execute(query_review)

# --- 3. Verification ---
count_listing = db.execute(f"SELECT count(*) FROM {table_name_listing}").fetchone()[0]
print(f"✅ Success! DuckDB ingested {count_listing} rows of {table_name_listing}!")

count_review = db.execute(f"SELECT count(*) FROM {table_name_review}").fetchone()[0]
print(f"✅ Success! DuckDB ingested {count_review} rows of {table_name_review}!")

db.close()

# --- 4. Cleanup Phase ---
print("🧹 Cleaning up temporary download files...")
if os.path.exists(local_listing_file):
    os.remove(local_listing_file)
if os.path.exists(local_review_file):
    os.remove(local_review_file)

print("🎉 Pipeline Complete! Check your _duckdb-data folder.")