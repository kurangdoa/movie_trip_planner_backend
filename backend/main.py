from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
from shared.schema import MovieHotelMatch, SearchDeps
import duckdb
import os
from backend.agent import run_orchestrator # Import your existing agent

app = FastAPI(root_path="/api")

# Enable CORS so your React frontend can talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",   # Local browser access
        "http://frontend:3000",     # Internal Docker network access
        "*",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    prompt: str
    city: str | None = None

    # This forces Swagger UI to show this exact JSON box!
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "prompt": "Medieval feeling of Game of Thrones",
                "city": "Amsterdam"
            }
        }
    )

@app.get("/")
async def root():
    return {"status": "online", "message": "Movie Hotel Recommender API is running"}

@app.post("/api/recommend", response_model=MovieHotelMatch)
async def get_recommendation(request: ChatRequest):
    try:
        # Run your Pydantic AI Agent
        result = await run_orchestrator(
                        user_prompt=request.prompt, 
                        city=request.city
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/cities")
def get_cities():
    DATA_DIR = "_duckdb-data"
    db_path = os.path.join(DATA_DIR, "hotel_movie.db")
    
    # 1. Use read_only=True to prevent Docker/API lock crashes
    # 2. Use 'with' so Python automatically runs db.close() when finished!
    with duckdb.connect(db_path, read_only=True) as db:
        query = "SELECT DISTINCT city FROM airbnb_listing_raw WHERE city IS NOT NULL ORDER BY city"
        results = db.execute(query).fetchall()
        
        cities = [row[0] for row in results]
    
    return {"cities": cities}