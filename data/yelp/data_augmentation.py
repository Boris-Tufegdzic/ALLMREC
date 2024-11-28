import ijson
import torch
import json
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM


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


def generate_description(prompt_template, model, tokenizer, device="cpu", max_length=100):
    input_ids = tokenizer(prompt_template, return_tensors="pt").input_ids.to(device)
    output = model.generate(input_ids, max_new_tokens=max_length, do_sample=True, top_p=0.9, temperature=0.7)
    return tokenizer.decode(output[0], skip_special_tokens=True)

def process_businesses(business_file, reviews_file, output_file, model, tokenizer, device, subset_size=None):
    processed = []
    business_generator = stream_businesses(business_file)
    
    for i, business in enumerate(tqdm(business_generator)):
        if subset_size and i >= subset_size:
            break
        most_useful_review = find_most_useful_review(reviews_file, business["business_id"])
        if not most_useful_review:
            description = "No useful reviews available."
        else:
            prompt = (
                "Write a concise and informative description for a business based on the following information.\n"
                f"Business Name: {business['name']}\n"
                f"Categories: {business['categories']}\n"
                f"Attributes: {business.get('attributes', 'None')}\n"
                f"Customer Feedback: {most_useful_review['text']}\n"
                f"Business Description:"
            )
            description = generate_description(prompt, model, tokenizer, device)
        business["description"] = description
        processed.append(business)
        
        # Write in chunks to avoid losing progress
        if i % 100 == 0:
            with open(output_file, "w") as f:
                json.dump(processed, f, indent=4)

    # Final save
    with open(output_file, "w") as f:
        json.dump(processed, f, indent=4)

def main():
    business_file = "/kaggle/input/yelpdata4/philadelphia/philadelphia_businesses.json"
    reviews_file = "/kaggle/input/yelpdata4/philadelphia/philadelphia_reviews.json"
    output_file = "/kaggle/working/ALLMREC/data/yelp/philadelphia_businesses_augmented.json"

    # Load model and tokenizer
    model_name = "facebook/opt-6.7b"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForCausalLM.from_pretrained(model_name).to("cuda" if torch.cuda.is_available() else "cpu")

    # Process businesses
    print("Processing businesses...")
    process_businesses(
        business_file,
        reviews_file,
        output_file,
        model,
        tokenizer,
        device="cuda" if torch.cuda.is_available() else "cpu",
        subset_size=10  # Remove or increase to process all businesses
    )

if __name__ == "__main__":
    main()
