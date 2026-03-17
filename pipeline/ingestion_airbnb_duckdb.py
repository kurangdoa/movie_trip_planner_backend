import urllib.request
import duckdb
import os

# 📁 1. Define and Create your Data Directory
DATA_DIR = "_duckdb-data"
os.makedirs(DATA_DIR, exist_ok=True) # Creates the folder safely!

# --- Configuration ---
urls_listing = ["https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-09-11/data/listings.csv.gz"
               , "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz"
                ]
urls_review = ["https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-09-11/data/reviews.csv.gz"
              , "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/reviews.csv.gz"
                ]

def ingestion_airbnb(url_listing:str
                     , url_review:str
                     ):

    table_name_listing = "airbnb_listing_raw"
    table_name_review = "airbnb_review_raw"

    # --- Extract Metadata ---
    city_listing = url_listing.split("/")[-4]
    province_listing = url_listing.split("/")[-5]
    country_listing = url_listing.split("/")[-6]

    city_review = url_review.split("/")[-4]
    province_review = url_review.split("/")[-5]
    country_review = url_review.split("/")[-6]

    # 📁 2. Point all files inside the new directory
    local_listing_file = os.path.join(DATA_DIR, f"temp_{city_listing}_listings.csv.gz")
    local_review_file = os.path.join(DATA_DIR, f"temp_{city_listing}_reviews.csv.gz")
    db_path = os.path.join(DATA_DIR, "hotel_movie.db")

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

    # --- 2. Database Phase (UPSERT LOGIC) ---
    print("  🚀 Upserting local files into DuckDB (Replacing by ID)...")

    # Check if the main listing table already exists
    tables_exist = db.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name_listing}'").fetchone()[0] > 0

    if not tables_exist:
        # 🟢 FIRST RUN: Create the tables from scratch
        db.execute(f"""
            CREATE TABLE {table_name_listing} AS 
            SELECT *, '{city_listing}' AS city, '{province_listing}' AS province, '{country_listing}' AS country 
            FROM read_csv_auto('{local_listing_file}', all_varchar=true)
        """)
        
        db.execute(f"""
            CREATE TABLE {table_name_review} AS 
            SELECT *, '{city_review}' AS city, '{province_review}' AS province, '{country_review}' AS country 
            FROM read_csv_auto('{local_review_file}', all_varchar=true)
        """)
    else:
        # 🟡 SUBSEQUENT RUNS: Replace based on ID using a Temp Table
        
        # -- LISTINGS UPSERT --
        db.execute(f"""
            CREATE TEMP TABLE temp_listing AS 
            SELECT *, '{city_listing}' AS city, '{province_listing}' AS province, '{country_listing}' AS country 
            FROM read_csv_auto('{local_listing_file}', all_varchar=true);
            
            DELETE FROM {table_name_listing} WHERE id IN (SELECT id FROM temp_listing);
            INSERT INTO {table_name_listing} SELECT * FROM temp_listing;
            DROP TABLE temp_listing;
        """)

        # -- REVIEWS UPSERT --
        db.execute(f"""
            CREATE TEMP TABLE temp_review AS 
            SELECT *, '{city_review}' AS city, '{province_review}' AS province, '{country_review}' AS country 
            FROM read_csv_auto('{local_review_file}', all_varchar=true);
            
            DELETE FROM {table_name_review} WHERE id IN (SELECT id FROM temp_review);
            INSERT INTO {table_name_review} SELECT * FROM temp_review;
            DROP TABLE temp_review;
        """)

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

# Trigger the function

counter=0
for url_listing, url_review in zip(urls_listing, urls_review):
    table_name_listing = "airbnb_listing_raw"
    table_name_review = "airbnb_review_raw"
    db_path = os.path.join(DATA_DIR, "hotel_movie.db")
    db = duckdb.connect(db_path)
    tables_exist = db.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name_listing}'").fetchone()[0] > 0
    if counter == 0:
        if tables_exist:
            db.execute(f"""DROP TABLE {table_name_listing}""")
            db.execute(f"""DROP TABLE {table_name_review} """)
        ingestion_airbnb(url_listing, url_review)
    else:
        ingestion_airbnb(url_listing, url_review)
    counter+=1
