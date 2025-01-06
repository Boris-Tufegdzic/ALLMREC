import torch
import json
from tqdm import tqdm
from transformers import pipeline



def stream_businesses(file_path):
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()  # Remove leading/trailing whitespace
            if not line:  # Skip empty lines
                continue
            try:
                business = json.loads(line)  # Parse each line as a JSON object
                yield {
                    "business_id": business["business_id"],
                    "name": business["name"],
                    "categories": business.get("categories"),
                    "attributes": business.get("attributes"),
                }
            except json.JSONDecodeError as e:
                # Log or print the error for debugging and skip the problematic line
                print(f"Skipping line due to JSONDecodeError: {e}")


def find_most_useful_review(reviews_file, business_id):
    most_useful = None
    with open(reviews_file, "r") as f:
        for line in f:
            line = line.strip()  # Remove leading/trailing whitespace
            if not line:  # Skip empty lines
                continue
            try:
                review = json.loads(line)  # Parse each line as a JSON object
                if review["business_id"] == business_id:
                    if not most_useful or review["useful"] > most_useful["useful"]:
                        most_useful = {
                            "text": review["text"],
                            "useful": review["useful"],
                        }
            except json.JSONDecodeError as e:
                # Log or print the error for debugging and skip the problematic line
                print(f"Skipping line due to JSONDecodeError: {e}")
    return most_useful

# Load Llama-3.2-1B
model_id = "meta-llama/Llama-3.2-3B-Instruct"
pipe = pipeline(
    "text-generation", 
    model=model_id, 
    torch_dtype=torch.bfloat16, 
    device_map="auto"
)

def generate_description(prompt, pipe, max_new_tokens=150, top_p=0.9, temperature=0.7):
    """
    Generates a description using the specified Llama pipeline.
    """
    messages = [
        {"role": "system", "content": "You are a helpful assistant that extracts objective information from reviews."},
        {"role": "user", "content": prompt},
    ]
    response = pipe(
        messages, 
        max_new_tokens=max_new_tokens, 
        top_p=top_p, 
        temperature=temperature
    )
    return response[0]["generated_text"]

def process_businesses(business_file, reviews_file, output_file, pipe, subset_size=None):
    """
    Process businesses and generate descriptions using Llama-3.2-1B.
    """
    processed = []
    business_generator = stream_businesses(business_file)
    
    for i, business in enumerate(tqdm(business_generator)):
        if subset_size and i >= subset_size:
            break

        # Get the most useful review for the business
        most_useful_review = find_most_useful_review(reviews_file, business["business_id"])
        if not most_useful_review:
            description = "No useful reviews available."
        else:
            prompt = (
                f"Extract the objective information from the following text: {most_useful_review['text']}."
            )
            description = generate_description(prompt, pipe)
            # Strip the prompt from the response
            #description = description.replace(prompt, "").strip()
        
        # Add the description to the business entry
        business["description"] = description
        processed.append(business)
        
        # Save progress in chunks
        if i % 100 == 0:
            with open(output_file, "w") as f:
                json.dump(processed, f, indent=4)

    # Final save
    with open(output_file, "w") as f:
        json.dump(processed, f, indent=4)

def main():
    # Run the pipeline
    business_file = "/kaggle/input/yelpdata4/philadelphia/philadelphia_businesses.json"
    reviews_file = "/kaggle/input/yelpdata4/philadelphia/philadelphia_reviews.json"
    output_file = "/kaggle/working/ALLMREC/data/yelp/philadelphia_businesses_augmented.json"

    print("Processing businesses...")
    process_businesses(
        business_file=business_file,
        reviews_file=reviews_file,
        output_file=output_file,
        pipe=pipe,
        subset_size=10  # Test on a smaller subset
    )



if __name__ == "__main__":
    main()
