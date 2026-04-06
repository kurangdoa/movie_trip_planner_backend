import asyncio
from pydantic_ai import Agent, RunContext, UsageLimits
from shared.schema import ArchitectOutput, MovieHotelMatch, SearchDeps
from shared.database import ChromaClient
import httpx
import os
import mlflow
from dotenv import load_dotenv

load_dotenv()

chroma_db = ChromaClient()

from langfuse import get_client
from langfuse import Langfuse, observe
from pydantic_ai.models.openrouter import OpenRouterModel
from pydantic_ai.providers.openrouter import OpenRouterProvider
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai import Agent
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.providers.anthropic import AnthropicProvider
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider


# mlflow
mlflow.pydantic_ai.autolog()
tracking_uri = mlflow.get_tracking_uri()
print(f"Current tracking uri: {tracking_uri}")
mlflow.set_experiment("movie-trip")

# langfuse
langfuse = get_client()
 
# Verify connection
if langfuse.auth_check():
    print("Langfuse client is authenticated and ready!")
else:
    print("Authentication failed. Please check your credentials and host.")



# model = OpenAIChatModel('gpt-4o-mini', provider=OpenAIProvider(api_key=os.getenv("OPENAI_API_KEY")))

# model = AnthropicModel(
#     'claude-sonnet-4', provider=AnthropicProvider(api_key=os.getenv("ANTHROPIC_API_KEY"))
# )

# model = OpenRouterModel(
#     'anthropic/claude-sonnet-4-5',
#     provider=OpenRouterProvider(api_key=os.getenv("OPENROUTER_API_KEY"))
# )

async def get_tmdb_details(movie_title: str) -> str:
    """Search TMDB for a movie and return the full poster URL."""
    tmdb_api_key = os.getenv("TMDB_API_KEY")
    tmdb_api_read_access_token = os.getenv("TMDB_API_READ_ACCESS_TOKEN")
    base_url = "https://api.themoviedb.org/3"
    image_base = "https://image.tmdb.org/t/p/w500" # w500 is a good size for web
    fallback = {"poster": "https://via.placeholder.com/500x750?text=No+Poster+Found", "overview": "No overview available.", "movie_url": ""}
    
    async with httpx.AsyncClient() as client:
        try:
            # Step 1: Search for the movie
            search_res = await client.get(
                f"{base_url}/search/multi",
                headers = {
                        "Authorization": f"Bearer {tmdb_api_read_access_token}",
                        "Content-Type": "application/json"
                    },
                params={"api_key": tmdb_api_key, "query": movie_title}
            )
            data = search_res.json()

            results = data.get("results", [])
            if results:
                # Sort so the actual famous movie/show is at the top of the list
                results.sort(key=lambda x: x.get("popularity", 0), reverse=True)
                
                # Grab the first one that has a valid poster
                for res in results:
                    poster_path = res.get("poster_path")
                    if poster_path:
                        media_type = res.get("media_type", "movie")
                        item_id = res.get("id")
                        movie_url = f"https://www.themoviedb.org/{media_type}/{item_id}" if item_id else ""

                        return {
                            "poster": f"{image_base}{poster_path}",
                            "overview": res.get("overview", "No overview available."),
                            "movie_url": movie_url
                        }
            return fallback
        except Exception as e:
            print(f"TMDB Error: {e}")
            return fallback

model = GoogleModel('gemini-2.5-flash'
                        #'gemini-2.5-flash'
                        #'gemini-2.5-pro'
                        # 'google-gla:gemini-2.5-flash-lite'
                        # 'google-gla:gemma-3-27b-it'
                        # 'google-gla:gemini-3-flash-preview'
                        # 'google-gla:gemini-2.0-flash'
                        # 'gemini-3-pro-preview'
                    , provider = GoogleProvider(api_key=os.getenv("GOOGLE_API_KEY"))
)

# --- 2. THE ARCHITECT (AGENT A) ---
# Goal: Translate "Movie Title" -> "Interior Design Keywords"

langfuse = Langfuse()

try:
    lf_architect = langfuse.get_prompt("movie-trip/architect_prompt", label="production")
    lf_scout = langfuse.get_prompt("movie-trip/scout_prompt", label="production")
    architect_prompt_text = lf_architect.compile()
    scout_prompt_text = lf_scout.compile()

except Exception as e:
    architect_prompt_text = (
        "You are an elite cinematic location scout. "
        "Your job is two-fold: "
        "1. Identify the ACTUAL movie title from the user's prompt (remove descriptions like 'vibe of' or 'feeling like'). "
        "2. Your other job is to translate a movie's overarching aesthetic into realistic AIRBNB AND HOTEL search keywords. "
        "You must capture the ENTIRE vibe of the experience: the interior design, the exterior architecture, the neighborhood vibe, the outside surroundings, and the atmospheric mood. "
        "For example, translate an epic/fantasy movie into 'historic stone exterior, dark wood beams, roaring fireplace, secluded forest surroundings, foggy mountainous region'. "
        "Translate a gritty neo-noir movie into 'sleek minimalist interior, low-light, neon-lit urban district, bustling city streets, rain-slicked pavement'. "
        "Output ONLY a comma-separated list of 5-8 physical, realistic keywords that describe the property and its immediate surroundings. "
        "DO NOT use metaphors or unsearchable terms like 'battle-hardened' or 'dragon-glass'."
        "Output ONLY the JSON with 'movie_title' and 'visual_dna'."
    ) 
    scout_prompt_text=(
        "You are a Cinematic Travel Curator. "
        "1. You will receive a Movie Title, a Destination, and its 'Vibe DNA' (keywords). "
        "2. MANDATORY: You MUST call the 'search_database' tool using those keywords to find properties in that destination. "
        "3. VIBE MATCHING: Even if there isn't a literal 100% match (e.g., no real spaceships), "
        "you MUST pick EXACTLY 3 properties from the tool results that BEST captures the *mood* and *atmosphere* of the movie. "
        "Do not return 1 or 2. Even if the matches are not perfect, pick the 3 best options and creatively justify why they fit the vibe."
        "4. SCORING: Provide a 'vibe_score' (e.g., 75/100) reflecting how well the property aligns with the movie's aesthetic. "
        "5. EXPLAIN: Creatively justify the matchin a fun, creative way, but use simple, everyday language. "
        "You MUST mention specific interior design features AND the exterior/neighborhood vibe from the listing to explain why it evokes the movie. "
        "6. EXACT DATA: Extract 'listing_url', 'picture_url', and 'property_name' EXACTLY as provided by the tool. DO NOT invent or hallucinate properties. If the tool returns nothing, state that no match was found. "
        "7. MOVIE TITLE: Set the 'movie_title' field to the EXACT title provided in the prompt. Do not guess or change it. "
        "8. FORMAT: You must output exactly 3 items in your matches list."
    ),


# Agent.instrument_all()
architect_agent = Agent(
    model,
    output_type=ArchitectOutput,
    name="architect_agent",
    system_prompt=architect_prompt_text,
    instrument=True
)

# --- 3. THE SCOUT (AGENT B - THE ORCHESTRATOR) ---
# Goal: Take keywords -> Search Database -> Pick Winner
scout_agent = Agent(
    model,
    deps_type=SearchDeps,
    output_type=MovieHotelMatch,
    name="scout_agent",
    system_prompt=scout_prompt_text,
    instrument=True
)

@scout_agent.tool
async def search_database(ctx: RunContext[SearchDeps], visual_description: str) -> list[dict]:
    normalized_city = ctx.deps.city
    print(f"🕵️ Scout searching in {normalized_city} for DNA: {visual_description}")
    return chroma_db.search_chroma_airbnb_by_vibe(
        collection_name="airbnb_listing",
        vibe_description=visual_description,
        n_results=10,
        city=normalized_city
    )

# --- 3. THE V3 TRACKING WRAPPERS ---
@mlflow.trace(name="architect_generation")
@observe(as_type="generation", name="architect_generation")
async def run_architect_with_tracking(user_prompt: str):
    # 🔗 Langfuse v3: Call update directly on the client!
    langfuse.update_current_generation(prompt=lf_architect)
    return await architect_agent.run(user_prompt, model_settings={"temperature": 0.3})

@mlflow.trace(name="scout_generation")
@observe(as_type="generation", name="scout_generation")
async def run_scout_with_tracking(prompt_to_scout: str, city: str):
    # 🔗 Langfuse v3: Call update directly on the client!
    langfuse.update_current_generation(prompt=lf_scout)
    return await scout_agent.run(
        prompt_to_scout,
        deps=SearchDeps(city=city),
        usage_limits=UsageLimits(request_limit=5)
    )

# --- 4. THE ORCHESTRATION LOGIC ---
@mlflow.trace(name="movie_trip_orchestrator")
@observe(name="movie_trip_orchestrator")
async def run_orchestrator(user_prompt: str, city: str):
    print(f"🎨 Step 1: Architect is building visual DNA for prompt '{user_prompt}'...")
    vibe_result = await run_architect_with_tracking(user_prompt)
    
    clean_movie_title = vibe_result.output.movie_title
    visual_dna = vibe_result.output.visual_dna
    
    print(f"🎬 Clean Title: {clean_movie_title}")
    print(f"🧬 DNA Created: {visual_dna}")

    # 🚀 THE FIX: Pass the movie title into the prompt!
    prompt_to_scout = (
            f"Movie Title: '{clean_movie_title}'\n"
            f"Visual DNA: {visual_dna}\n\n"
            f"Find EXACTLY 3 properties matching this DNA in {city}."
        )

    # print(f"🚀 Step 2: Scout is finding the match in {city}...")
    # final_result = await scout_agent.run(
    #     prompt_to_scout,
    #     deps=SearchDeps(city=city),
    #     # model_settings={"max_tokens": 1000},
    #     usage_limits=UsageLimits(request_limit=5),
    # )

    scout_task = run_scout_with_tracking(prompt_to_scout, city)
    tmdb_task = get_tmdb_details(clean_movie_title)
    
    scout_run_result, tmdb_info = await asyncio.gather(scout_task, tmdb_task)

    final_match = scout_run_result.output
    
    final_match.movie_poster = tmdb_info["poster"]
    final_match.movie_overview = tmdb_info["overview"]
    final_match.movie_url = tmdb_info["movie_url"]
    
    final_match.movie_title = clean_movie_title
    final_match.user_prompt = user_prompt
    
    return final_match

# --- TEST BLOCK ---
if __name__ == "__main__":
    async def test():
        match = await run_orchestrator("game of thrones", "Amsterdam")
        
        print("\n" + "="*50)
        print(f"🍿 MOVIE: {match.movie_title}")
        print(f"🖼️ POSTER: {match.movie_poster}")
        print(f"🏨 FOUND {len(match.matches)} MATCHES:")
        
        # 🟢 UPDATED: Loop through the 3 matches
        for idx, prop in enumerate(match.matches, 3):
            print(f"\n  --- Match #{idx} ---")
            print(f"  Score: {prop.vibe_score}/100")
            print(f"  Vibe Check: {prop.explanation}")
            print(f"  Book It: {prop.listing_url}")
            
        print("="*50 + "\n")

    asyncio.run(test())