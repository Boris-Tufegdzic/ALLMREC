import json
import random
import os

def filter_yelp_data(business_file, review_file, filtered_business_file, filtered_review_file, 
                     business_threshold=50, user_threshold=10):
    """
    Optimized single-pass filtering of Yelp dataset
    
    Key Optimizations:
    1. Combines business and review filtering 
    2. Tracks user review counts simultaneously
    """
    # Tracking sets and dictionaries
    philadelphia_business_ids = set()
    user_review_count = {}
    cities = ["Philadelphia", "New Orleans", "Nashville", "Reno"]
    
    # Open all files in a single context
    with open(business_file, "r", encoding="utf-8") as b_file, \
         open(review_file, "r", encoding="utf-8") as r_file, \
         open(filtered_business_file, "w", encoding="utf-8") as filtered_b_file, \
         open(filtered_review_file, "w", encoding="utf-8") as filtered_r_file:
        
        # First pass: Filter businesses and prepare for review filtering
        for line in b_file:
            business = json.loads(line)
            
            # Business filtering criteria
            if business["review_count"] >= business_threshold and business["city"] in cities:
                # Write filtered businesses
                json.dump(business, filtered_b_file)
                filtered_b_file.write("\n")
                philadelphia_business_ids.add(business["business_id"])
        
        # Reset file pointer for review file
        r_file.seek(0)
        
        # First sub-pass: Count user reviews
        for line in r_file:
            review = json.loads(line)
            
            # Only count reviews for valid businesses
            if review["business_id"] in philadelphia_business_ids:
                user_id = review["user_id"]
                user_review_count[user_id] = user_review_count.get(user_id, 0) + 1
        
        # Reset file pointer again
        r_file.seek(0)
        
        # Second pass: Filter reviews based on business and user criteria
        for line in r_file:
            review = json.loads(line)
            
            # Review filtering criteria
            if (review["business_id"] in philadelphia_business_ids and 
                user_review_count.get(review["user_id"], 0) >= user_threshold):
                json.dump(review, filtered_r_file)
                filtered_r_file.write("\n")
    
    print("Data filtering completed successfully!")


random.seed(42)

kaggle_input_dir = "/kaggle/input/fullyelpdata/fullyelpdata/"
kaggle_output_dir = "/kaggle/working/ALLMREC/data/yelp/filtered/"
if not os.path.exists(kaggle_output_dir):
    os.makedirs(kaggle_output_dir)
# Input file paths
business_file = kaggle_input_dir + "yelp_academic_dataset_business.json"
review_file = kaggle_input_dir + "yelp_academic_dataset_review.json"

# Output file paths
filtered_business_file = kaggle_output_dir + "filtered_businesses.json"
filtered_review_file = kaggle_output_dir + "filtered_reviews.json"


filter_yelp_data(
    business_file=business_file,
    review_file=review_file,
    filtered_business_file=filtered_business_file,
    filtered_review_file=filtered_review_file,
)
