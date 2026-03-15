import os
import duckdb
from shared.database import ClickHouseClient, ChromaClient
from shared.schema import AirbnbListing

# print("🔌 Connecting to databases...")
# # Initialize both of your professional classes!
# ch_db = ClickHouseClient(database='hotel_movie')
# chroma_db = ChromaClient()

# # 1. Fetch the raw data directly from ClickHouse
# print("📥 Pulling raw data from ClickHouse...")
# query = """
# with review as
# (
# -- Get only the 5 most recent comments per listing
#     SELECT listing_id, comments 
#     FROM airbnb_review_raw 
#     ORDER BY date DESC
#     LIMIT 5 BY listing_id
# )
# SELECT 
#     l.id, 
#     l.name, 
#     l.description, 
#     l.neighborhood_overview,
#     l.price, 
#     l.property_type, 
#     l.listing_url,
#     l.picture_url,
#     l.city,
#     -- This squishes the 5 reviews into a single list of text!
#     groupArray(r.comments) AS recent_reviews 
# FROM airbnb_listing_raw as l
# LEFT JOIN review as r ON l.id = r.listing_id
# WHERE 
#     l.number_of_reviews >= 10
# GROUP BY 
#     l.id, l.name, l.description, l.neighborhood_overview, 
#     l.price, l.property_type, l.listing_url, l.picture_url, l.city
# """
# # ClickHouse securely runs the query and we turn it into standard Python dictionaries
# raw_records = ch_db.get_client().query_df(query).to_dict(orient='records')
# print("raw_records: ", raw_records[0])

print("🔌 Connecting to databases...")

# Connect directly to the local DuckDB file we created earlier
db_path = os.path.join("_duckdb-data", "hotel_movie.db")
db = duckdb.connect(db_path)

# Initialize your vector DB
chroma_db = ChromaClient()

# 1. Fetch the raw data directly from DuckDB
print("📥 Pulling raw data from DuckDB...")
query = """
WITH review AS (
    -- Get only the 5 most recent comments per listing using DuckDB Window Functions
    SELECT listing_id, comments
    FROM (
        SELECT 
            listing_id, 
            comments,
            ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY TRY_CAST(date AS DATE) DESC) as rn
        FROM airbnb_review_raw
    )
    WHERE rn <= 5
)
SELECT 
    l.id, 
    l.name, 
    l.description, 
    l.neighborhood_overview,
    l.price, 
    l.property_type, 
    l.listing_url,
    l.picture_url,
    l.city,
    -- DuckDB uses list() to squish rows into an array!
    list(r.comments) AS recent_reviews 
FROM airbnb_listing_raw as l
LEFT JOIN review as r ON l.id = r.listing_id
WHERE 
    -- Cast the string to an integer so the >= 10 logic works
    TRY_CAST(l.number_of_reviews AS INTEGER) >= 10
GROUP BY 
    l.id, l.name, l.description, l.neighborhood_overview, 
    l.price, l.property_type, l.listing_url, l.picture_url, l.city
"""

# DuckDB natively outputs to Pandas DataFrames, so the syntax is super clean
raw_records = db.execute(query).df().to_dict(orient='records')

print(f"📦 Successfully fetched {len(raw_records)} records!")
print("raw_records: ", raw_records[0])

# Close the database connection when done fetching
db.close()


# 2. Get the Chroma collection using your custom method
collection = chroma_db.get_chroma_collection(collection_name="airbnb_listing")

# 3. Clean and Validate with Pydantic
print("🛡️ Validating data and building text documents...")
ids = []
documents = []
metadatas = []

for row in raw_records:
    # Pydantic handles the type casting and safety checks!

    # clickhouse
    # recent_reviews_list = row.get('recent_reviews', [])
    # recent_reviews = " ".join(recent_reviews_list) if recent_reviews_list else ""

    raw_reviews = row.get('recent_reviews')

    # Check if it's an iterable array/list (and safely ignore empty float NaNs)
    if hasattr(raw_reviews, '__iter__') and not isinstance(raw_reviews, str):
        # Convert items to strings, ignoring 'nan' or empty values
        clean_reviews = [str(r) for r in raw_reviews if r and str(r).lower() != 'nan']
        recent_reviews = " ".join(clean_reviews)
    else:
        recent_reviews = ""

    record = AirbnbListing(
        id=str(row.get('id', '')),
        document=f"{row.get('name', '')} | {row.get('description', '')} | {row.get('neighborhood_overview', '')} | Located in {row.get('neighbourhood_cleansed', '')} | {row.get('property_type', '')} | reviews: {recent_reviews}",
        listing_url=str(row.get('listing_url', '')),
        picture_url=str(row.get('picture_url', '')),
        price=str(row.get('price', '$0')),
        # Catch empty ratings from the raw database
        rating=float(row.get('review_scores_rating', 0.0)) if row.get('review_scores_rating') else 0.0,
        neighborhood=str(row.get('neighbourhood_cleansed', 'Unknown')),
        city=str(row.get('city', 'Unknown')),
    )

    ids.append(record.id)
    documents.append(record.document)
    metadatas.append(record.get_metadata())

# 4. Push to ChromaDB in batches
print(f"📦 Total records to insert: {len(ids)}. Chunking data...")
BATCH_SIZE = 5000 

for i in range(0, len(ids), BATCH_SIZE):
    batch_ids = ids[i : i + BATCH_SIZE]
    batch_documents = documents[i : i + BATCH_SIZE]
    batch_metadatas = metadatas[i : i + BATCH_SIZE]
    
    print(f"🔄 Upserting batch from {i} to {i + len(batch_ids)}...")
    
    collection.upsert(
        ids=batch_ids,
        documents=batch_documents,
        metadatas=batch_metadatas
    )

print(f"✅ Successfully stocked the Vector fridge with {len(ids)} stays!")

# 5. Test it out using your built-in search method!
print("🔍 Testing a semantic search...")
# Notice how clean this is compared to writing raw Chroma queries!
test_results = chroma_db.search_chroma_airbnb_by_vibe(
    collection_name="airbnb_listing",
    vibe_description="A cozy, romantic apartment near the canals", 
    n_results=2
)

for result in test_results:
    print(f"\n🏠 Found: {result['vibe_text'][:100]}...")
    print(f"🔗 URL: {result['details']['listing_url']}")