import os
import chromadb
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

# --------------------
# chroma db
# --------------------

class ChromaClient:
    """Chroma Class"""
    def __init__(self):
        """Initializes the connection to ChromaDB automatically."""
        
        host = os.getenv("CHROMA_HOST", "localhost")
        # If we are in Docker, it will use 8000. If local, it falls back to 8201.
        port = int(os.getenv("CHROMA_PORT", 8201))
        self.client = chromadb.HttpClient(host=host, port=port)

    def get_chroma_collection(self, collection_name: str):
        """
        Connects to your specific Airbnb collection.
        By NOT passing an embedding_function, ChromaDB automatically 
        downloads and uses its default open-source model (all-MiniLM-L6-v2).
        """
        return self.client.get_or_create_collection(name=collection_name)

    def search_chroma_airbnb_by_vibe(self
                                     , collection_name: str
                                     , vibe_description: str
                                     , n_results: int = 3
                                     , city: str = None
                                     ) -> list[dict]:
        """
        This is the Tool your Gemini Agent will call.
        It takes a text description, searches the database, 
        and returns a clean, formatted list of matching hotels.
        """
        
        collection = self.get_chroma_collection(collection_name)
        # 🎯 THE MAGIC FILTER: Build the 'where' clause if a city is provided
        where_clause = {"city": city} if city else None

        # print(f"filtering {where_clause}")
        
        # Query ChromaDB - it will automatically embed your search text
        results = collection.query(
            query_texts=[vibe_description],
            n_results=n_results,
            include=['documents', 'metadatas'],
            where=where_clause
        )
        
        # Clean up ChromaDB's nested lists into a simple list of dictionaries
        formatted_results = []


        # Safety check in case the database is empty or returns nothing
        if not results['ids'] or not results['ids'][0]:
            return formatted_results

        for i in range(len(results['ids'][0])):
            formatted_results.append({
                "id": results['ids'][0][i],
                "vibe_text": results['documents'][0][i],
                "details": results['metadatas'][0][i]
            })

        # print(f"vector search result: {formatted_results}")
            
        return formatted_results


# --------------------
# clickhouse
# --------------------

# class ClickHouseClient:
#     def __init__(
#         self,
#         host=os.getenv("CLICKHOUSE_HOST", "localhost"),
#         port=int(os.getenv("CLICKHOUSE_PORT", 8202)),
#         username=os.getenv("CLICKHOUSE_USER", "app_user"),
#         password=os.getenv("CLICKHOUSE_APP_PASS"),
#         database=os.getenv("CLICKHOUSE_DB", "hotel")
#     ):
#         """Initializes the connection to ClickHouse automatically."""
#         self.client = clickhouse_connect.get_client(
#             host=host,
#             port=port,
#             username=username,
#             password=password,
#             database=database
#         )

#     def get_client(self):
#         """Returns the raw ClickHouse connection client."""
#         return self.client

#     def create_clickhouse_table(self, table_name: str, query: str):
#         """
#         Drops the table if it exists, then executes the table creation query.
#         """
#         self.client.command(f"DROP TABLE IF EXISTS {table_name}")
#         self.client.command(query)
#         print(f"✅ Table '{table_name}' created successfully!")