from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from shared.schema import MovieHotelMatch, SearchDeps
from backend.agent import run_orchestrator # Import your existing agent

app = FastAPI()

# Enable CORS so your React frontend can talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",   # Local browser access
        "http://frontend:3000"     # Internal Docker network access
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    prompt: str
    city: str | None = None

@app.get("/")
async def root():
    return {"status": "online", "message": "Movie Hotel Recommender API is running"}

@app.post("/api/recommend", response_model=MovieHotelMatch)
async def get_recommendation(request: ChatRequest):
    try:
        # Run your Pydantic AI Agent
        result = await run_orchestrator(
                        movie_title=request.prompt, 
                        city=request.city
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))