from agent import run_orchestrator
from dotenv import load_dotenv
from langfuse import get_client, Evaluation
from litellm import completion
import json

load_dotenv()
 
# Initialize client
langfuse = get_client()

orchestrator_dataset = langfuse.get_dataset("movie-trip/orchestrator_dataset")

def python_llm_judge_evaluator(*, output, expected_output, **kwargs):
    """
    Calls Gemini 2.5 Flash to act as an automated judge.
    """
    grading_prompt = (
        f"Compare the actual output to the expected output and score its accuracy.\n"
        f"Output: {output}\n"
        f"Expected: {expected_output}\n\n"
        f"Provide a score between 0.0 and 1.0. Also provide a brief, 1-2 sentence explanation of why you gave this score.\n"
        f"Respond STRICTLY with valid JSON in this exact format:\n"
        f'{{"score": 0.8, "reason": "The output matched the city but missed one of the hotel IDs."}}'
    )
    
    # Call Gemini via LiteLLM
    try:
        response = completion(
            model="gemini/gemini-2.5-flash", 
            messages=[{"role": "user", "content": grading_prompt}]
        )
        
        # Extract the raw text
        raw_response = response.choices[0].message.content.strip()

        if raw_response.startswith("```json"):
            raw_response = raw_response[7:-3].strip()
        elif raw_response.startswith("```"):
            raw_response = raw_response[3:-3].strip()
            
        parsed_data = json.loads(raw_response)
        score_value = float(parsed_data["score"])
        reason_text = parsed_data["reason"]
        
        return Evaluation(
            name="llm_accuracy", 
            value=score_value,
            comment=reason_text 
        )
    except Exception as e:
        print(f"Evaluation failed: {e}")
        return Evaluation(name="llm_accuracy", value=0.0, comment=f"Judge crashed or failed to parse JSON. Error: {e}")
    
async def orchestrator_wrapper(*, item, **kwargs):
    # Make sure these keys match the JSON in your Langfuse dataset 'input'
    user_prompt = item.input["user_prompt"]
    city = item.input["city"]
    
    # Hand the variables to your actual agent!
    return await run_orchestrator(user_prompt, city)

result = orchestrator_dataset.run_experiment(
    name="Movie Trip Orchestrator Evaluation",
    description="Testing full pipeline",
    task=orchestrator_wrapper,
    evaluators=[python_llm_judge_evaluator],
    max_concurrency=5  # Control concurrent API calls
)

print(result.format())