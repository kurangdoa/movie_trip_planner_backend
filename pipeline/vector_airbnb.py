import os
import duckdb
from shared.database import ChromaClient
import torch
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
import time

print("🔌 Connecting to databases...")

# Connect directly to the local DuckDB file
db_path = os.path.join("_duckdb-data", "hotel_movie.db")
db = duckdb.connect(db_path)

# 1. Auto-detect the best available hardware
if torch.backends.mps.is_available():
    device = "mps"   # Apple Silicon GPU
elif torch.cuda.is_available():
    device = "cuda"  # Nvidia GPU (common in cloud servers)
else:
    device = "cpu"   # Standard server/machine without a GPU

print(f"🧠 Loading AI Embedding Model onto {device.upper()}...")

# 2. Pass the detected device to the model
model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

chroma_db = ChromaClient()

# 1. Fetch the raw data and build the text document DIRECTLY in DuckDB
print("📥 Pulling and formatting data from DuckDB...")

query = """
SELECT 
    CAST(id AS VARCHAR) AS id, 
    COALESCE(listing_url, '') AS listing_url,
    COALESCE(picture_url, '') AS picture_url,
    COALESCE(price, '$0') AS price,
    COALESCE(city, 'Unknown') AS city,
    
    -- OPTIMIZATION 1: DuckDB does the string formatting in milliseconds!
    -- CONCAT_WS automatically ignores NULL values so your text stays clean.
    CONCAT_WS(' | ', 
        name, 
        description, 
        neighborhood_overview, 
        'Located in ' || city, 
        property_type
    ) AS document
FROM airbnb_listing_raw
WHERE description IS NOT NULL 
  AND document != '' -- Filter out totally empty rows
  AND city != 'None'
  AND CAST(number_of_reviews as INT) >= 10
"""

# Fetch directly to a Pandas DataFrame
df = db.execute(query).df()
print(f"📦 Successfully fetched and formatted {len(df)} records natively!")
db.close()

# 2. Get the Chroma collection
collection = chroma_db.get_chroma_collection(collection_name="airbnb_listing")

# 3. Vectorized Data Prep (Skipping the slow Python loop!)
print("⚡ Extracting data columns for Chroma...")

# 🚀 OPTIMIZATION 2: Convert pandas columns directly to lists. 
# This takes fractions of a second compared to looping over dictionaries.
ids = df['id'].tolist()
documents = df['document'].tolist()

# Create metadata dictionaries instantly
metadatas = df[['listing_url', 'picture_url', 'price', 'city']].to_dict(orient='records')

# 4. Push to ChromaDB in batches
BATCH_SIZE = 2500 # 2500 is the sweet spot. 5000 can sometimes trigger payload size limits on embedding APIs.
print(f"📦 Total records to insert: {len(ids)}. Chunking data...")

for i in tqdm(range(0, len(ids), BATCH_SIZE), desc="Upserting Batches"):
    batch_ids = ids[i : i + BATCH_SIZE]
    batch_docs = documents[i : i + BATCH_SIZE]
    batch_metas = metadatas[i : i + BATCH_SIZE]
    
    # ⚡ CALCULATE VECTORS MANUALLY (This is insanely fast on a Mac GPU)
    batch_embeddings = model.encode(batch_docs, show_progress_bar=False)
    
    collection.upsert(
        ids=batch_ids,
        embeddings=batch_embeddings.tolist(),
        documents=batch_docs,
        metadatas=batch_metas
    )

    time.sleep(2)

print(f"✅ Successfully stocked the Vector fridge with {len(ids)} stays!")

# 5. Test it out
print("🔍 Testing a semantic search...")
test_results = chroma_db.search_chroma_airbnb_by_vibe(
    collection_name="airbnb_listing",
    vibe_description="A cozy, romantic apartment near the canals", 
    n_results=2
)

for result in test_results:
    print(f"\n🏠 Found: {result['vibe_text'][:100]}...")
    print(f"🔗 URL: {result['details']['listing_url']}")

# ==========================================
# 🧹 CLEANING UP GHOST LISTINGS
# ==========================================
print("🧹 Starting ChromaDB cleanup sync...")

# 1. Get all active IDs from DuckDB 
# (Make sure they are strings so they match ChromaDB's format!)
active_duckdb_ids = set(df['id'].astype(str).tolist())

# 2. Get all IDs currently sitting in ChromaDB using Pagination
existing_chroma_ids = set()
offset = 0
chunk_size = 10000

print("🔍 Fetching existing IDs from ChromaDB...")
while True:
    # include=[] ensures we only pull the IDs, saving massive amounts of RAM
    batch = collection.get(limit=chunk_size, offset=offset, include=[])
    
    if not batch['ids']:
        break  # Reached the end
        
    existing_chroma_ids.update(batch['ids'])
    offset += chunk_size

# 3. Find the difference (IDs in Chroma that are no longer in DuckDB)
dead_ids = list(existing_chroma_ids - active_duckdb_ids)

# 4. Tell Chroma to delete them safely in batches
if dead_ids:
    print(f"🗑️ Found {len(dead_ids)} ghost listings! Deleting from Chroma...")
    
    # We must batch the deletions too, otherwise SQLite will crash again!
    delete_chunk_size = 5000
    for i in range(0, len(dead_ids), delete_chunk_size):
        chunk_to_delete = dead_ids[i : i + delete_chunk_size]
        collection.delete(ids=chunk_to_delete)
        
    print("✅ Clean up complete! No more ghost listings.")
else:
    print("✅ No ghost listings found. ChromaDB is perfectly synced!")