from pydantic import BaseModel
from dataclasses import dataclass

class MovieHotelMatch(BaseModel):
    hotel_id: str
    movie_title: str
    explanation: str
    vibe_score: int
    listing_url: str
    picture_url: str
    
# 1. We validate the ENTIRE row from Pandas
class AirbnbListing(BaseModel):
    id: str
    document: str  # The AI search text
    listing_url: str
    picture_url: str
    price: str
    rating: float = 0.0
    neighborhood: str
    city: str

    # 2. We add a helper method to easily extract just the metadata for Chroma
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