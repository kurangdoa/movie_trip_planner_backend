import asyncio
from pydantic_ai import Agent, RunContext, UsageLimits
from shared.schema import MovieHotelMatch, SearchDeps
from shared.database import ChromaClient
import os
from dotenv import load_dotenv

load_dotenv()

chroma_db = ChromaClient()

from pydantic_ai.models.openrouter import OpenRouterModel
from pydantic_ai.providers.openrouter import OpenRouterProvider
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai import Agent
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.providers.anthropic import AnthropicProvider
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

# model = OpenAIChatModel('gpt-4o-mini', provider=OpenAIProvider(api_key=os.getenv("OPENAI_API_KEY")))

# model = AnthropicModel(
#     'claude-sonnet-4', provider=AnthropicProvider(api_key=os.getenv("ANTHROPIC_API_KEY"))
# )

# model = OpenRouterModel(
#     'anthropic/claude-sonnet-4-5',
#     provider=OpenRouterProvider(api_key=os.getenv("OPENROUTER_API_KEY"))
# )

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
architect_agent = Agent(
    model,
    system_prompt=(
        "You are an elite cinematic interior designer. "
        "Your job is to translate a movie's aesthetic into realistic AIRBNB INTERIOR DESIGN keywords. "
        "Focus on mood, atmosphere, lighting, and realistic materials found in urban apartments. "
        "For example, translate epic/fantasy concepts into 'dark wood beams, moody lighting, "
        "antique furniture, historic charm, stone accents'. "
        "Output ONLY a comma-separated list of 5-7 physical, realistic architectural keywords. "
        "DO NOT use metaphors or unsearchable terms like 'battle-hardened' or 'dragon-glass'."
    )
)

# --- 3. THE SCOUT (AGENT B - THE ORCHESTRATOR) ---
# Goal: Take keywords -> Search Database -> Pick Winner
scout_agent = Agent(
    model,
    deps_type=SearchDeps,
    output_type=MovieHotelMatch,
    system_prompt=(
        "You are a Cinematic Travel Curator. "
        "1. You will receive a Movie Title and its 'Vibe DNA' (keywords). "
        "2. MANDATORY: You MUST call the 'search_database' tool using those keywords. "
        "3. VIBE MATCHING: Even if there isn't a literal 100% match (e.g., no real castles), "
        "you MUST pick the property that BEST captures the *mood* and *atmosphere* of the movie. "
        "4. SCORING: Provide a 'vibe_score' (e.g., 75/100) reflecting how well the atmosphere aligns. "
        "5. EXPLAIN: Creatively justify why this property's specific features evoke the movie's feeling. "
        "6. EXACT URLS: Extract 'listing_url' and 'picture_url' EXACTLY as provided by the tool."
        "7. MOVIE TITLE: Set the 'movie_title' field to the EXACT title provided in the prompt. Do not guess or change it."
    )
)

@scout_agent.tool
async def search_database(ctx: RunContext[SearchDeps], visual_description: str) -> list[dict]:
    normalized_city = ctx.deps.city.lower()
    print(f"🕵️ Scout searching in {normalized_city} for DNA: {visual_description}")
    return chroma_db.search_chroma_airbnb_by_vibe(
        collection_name="airbnb_listing",
        vibe_description=visual_description,
        n_results=10,
        city=normalized_city
    )

# --- 4. THE ORCHESTRATION LOGIC ---
async def run_orchestrator(movie_title: str, city: str):
    print(f"🎨 Step 1: Architect is building visual DNA for '{movie_title}'...")
    vibe_result = await architect_agent.run(movie_title
                                            , model_settings={"temperature": 0.3}
                                            )
    visual_dna = vibe_result.output
    print(f"🧬 DNA Created: {visual_dna}")

    # 🚀 THE FIX: Pass the movie title into the prompt!
    prompt_to_scout = f"Movie Title: '{movie_title}'\nVisual DNA: {visual_dna}\n\nFind a property matching this DNA in {city}."

    print(f"🚀 Step 2: Scout is finding the match in {city}...")
    final_result = await scout_agent.run(
        prompt_to_scout,
        deps=SearchDeps(city=city),
        # model_settings={"max_tokens": 1000},
        usage_limits=UsageLimits(request_limit=5),
    )
    
    return final_result.output

# --- TEST BLOCK ---
if __name__ == "__main__":
    async def test():
        match = await run_orchestrator("game of thrones", "Amsterdam")
        print(f"\n✅ MATCH FOUND: {match.hotel_id} - {match.explanation}")
            # Extract the typed Pydantic object
        
        # Print it out beautifully
        print("\n" + "="*50)
        print(f"🍿 MOVIE: {match.movie_title}")
        print(f"🏨 SCORE: {match.vibe_score}/100")
        print(f"📝 VIBE CHECK: {match.explanation}")
        print(f"🔗 BOOK IT: {match.listing_url}")
        print(f"🖼️ SEE IT: {match.picture_url}")
        print("="*50 + "\n")

    asyncio.run(test())