from shared.database import ClickHouseClient

url_listing = "http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2023-12-12/data/listings.csv.gz"
url_review = "http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2023-12-12/data/reviews.csv.gz"
table_name_listing = "airbnb_listing_raw"
table_name_review = "airbnb_review_raw"
city_listing = url_listing.split("/")[-4]
province_listing = url_listing.split("/")[-5]
country_listing = url_listing.split("/")[-6]
city_review = url_review.split("/")[-4]
province_review = url_review.split("/")[-5]
country_review = url_review.split("/")[-6]
print(city_listing  + " | " + province_listing + " | " + country_listing)

print("🔌 Connecting to database...")
# 1. Initialize our new Object-Oriented database client
ch_client = ClickHouseClient(database='hotel_movie') 

print("🚀 Telling ClickHouse to download and ingest the CSV directly...")

# 2. The Foolproof Query (Using tuple() to bypass nullable errors)
query_listing = f"""
    CREATE TABLE {table_name_listing} 
    ENGINE = MergeTree() 
    ORDER BY tuple() 
    AS SELECT
        *, 
        '{city_listing}' AS city, 
        '{province_listing}' AS province, 
        '{country_listing}' AS country 
    FROM url(
        '{url_listing}', 
        'CSVWithNames'
    )
"""

query_review = f"""
    CREATE TABLE {table_name_review} 
    ENGINE = MergeTree() 
    ORDER BY tuple() 
    AS SELECT 
        *, 
        '{city_review}' AS city, 
        '{province_review}' AS province, 
        '{country_review}' AS country 
    FROM url(
        '{url_review}', 
        'CSVWithNames'
    )
"""

# 3. Use the class method to safely drop and recreate the table
ch_client.create_clickhouse_table(table_name=table_name_listing, query=query_listing)
ch_client.create_clickhouse_table(table_name=table_name_review, query=query_review)

# 4. Verify it worked using the raw client connection
count = ch_client.get_client().command(f"SELECT count(*) FROM {table_name_listing}")
print(f"✅ Success! ClickHouse independently downloaded and ingested {count} rows of {table_name_listing}!")
count = ch_client.get_client().command(f"SELECT count(*) FROM {table_name_review}")
print(f"✅ Success! ClickHouse independently downloaded and ingested {count} rows of {table_name_review}!")
