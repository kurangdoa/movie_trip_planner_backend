from pydantic import BaseModel, Field
from dataclasses import dataclass
class PropertyMatch(BaseModel):
    hotel_id: str
    vibe_score: int
    explanation: str
    listing_url: str
    picture_url: str

class MovieHotelMatch(BaseModel):
    user_prompt: str | None = None
    movie_title: str
    movie_poster: str | None = None
    movie_overview: str | None = None
    movie_url: str | None = None 
    # Tell Pydantic AI to return a list of matches
    matches: list[PropertyMatch] = Field(description="A list of up to 3 matching properties")

class ArchitectOutput(BaseModel):
    movie_title: str
    visual_dna: str

class AirbnbListing(BaseModel):
    id: str
    document: str  # The AI search text
    listing_url: str
    picture_url: str
    price: str
    rating: float = 0.0
    neighborhood: str
    city: str

    def get_metadata(self) -> dict:
        return {
            "listing_url": self.listing_url,
            "picture_url": self.picture_url,
            "price": self.price,
            "rating": self.rating,
            "neighborhood": self.neighborhood,
            "city": self.city
        }

@dataclass
class SearchDeps:
    city: str | None