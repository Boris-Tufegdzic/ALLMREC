import json

# Input file paths
business_file = "yelp_academic_dataset_business.json"
review_file = "yelp_academic_dataset_review.json"
user_file = "yelp_academic_dataset_user.json"

# Output file paths
filtered_business_file = "philadelphia/philadelphia_businesses.json"
half_filtered_review_file = "philadelphia/philadelphia_hf_reviews.json"
filtered_review_file = "philadelphia/philadelphia_reviews.json"
filtered_user_file = "philadelphia/philadelphia_users.json"

# Step 1: Filter businesses in Philadelphia
philadelphia_business_ids = set()
business_threshold = 10

with open(business_file, "r", encoding="utf-8") as infile, open(filtered_business_file, "w", encoding="utf-8") as outfile:
    for line in infile:
        business = json.loads(line)
        if business["city"].lower() == "philadelphia" and business["review_count"] >= business_threshold:
            json.dump(business, outfile)
            outfile.write("\n")
            philadelphia_business_ids.add(business["business_id"])

# Step 2: Filter reviews of Philadelphia businesses
philadelphia_user_ids = set()

with open(review_file, "r", encoding="utf-8") as infile, open(half_filtered_review_file, "w", encoding="utf-8") as outfile:
    for line in infile:
        review = json.loads(line)
        if review["business_id"] in philadelphia_business_ids:
            json.dump(review, outfile)
            outfile.write("\n")
            philadelphia_user_ids.add(review["user_id"])

# Step 3: Filter users who wrote at least {user_threshold} reviews for Philadelphia businesses

user_threshold = 5
# Read Philadelphia reviews and count user reviews in Philadelphia
user_review_count = {}
with open(half_filtered_review_file, "r", encoding="utf-8") as review_file:
    for line in review_file:
        review = json.loads(line)
        user_id = review["user_id"]
        user_review_count[user_id] = user_review_count.get(user_id, 0) + 1

# U to the new JSON file
filtered_user_ids = set()
with open(filtered_user_file, "w", encoding="utf-8") as user_outfile:
    with open(user_file, "r", encoding="utf-8") as user_file:
        for line in user_file:
            user = json.loads(line)
            if user_review_count.get(user["user_id"], 0) >= user_threshold:
                user_outfile.write(json.dumps(user) + "\n")
                filtered_user_ids.add(user["user_id"])

# Filter reviews based on filtered_user_ids
with open(half_filtered_review_file, "r", encoding="utf-8") as infile, open(filtered_review_file, "w", encoding="utf-8") as outfile:
    for line in infile:
        review = json.loads(line)
        if review["user_id"] in filtered_user_ids:
            json.dump(review, outfile)
            outfile.write("\n")


print("Filtered data has been saved successfully!")
