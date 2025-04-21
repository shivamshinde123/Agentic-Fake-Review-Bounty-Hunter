import csv
import json
import pandas as pd

def convert_businesses_json_to_csv(json_file_path, csv_file_path):
    fieldnames = ["business_id", "name", "city", "state", "stars", "review_count"]

    with open(json_file_path, 'r', encoding='utf-8') as infile, \
         open(csv_file_path, 'w', newline='', encoding='utf-8') as outfile:

        writer = csv.DictWriter(
            outfile,
            fieldnames=fieldnames,
            delimiter=',',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\'
        )
        writer.writeheader()

        for line_num, line in enumerate(infile, 1):
            try:
                data = json.loads(line)
                # Safely get each field and verify all are strings/numbers
                row = {
                    "business_id": data["business_id"],
                    "name": data["name"],
                    "city": data["city"],
                    "state": data["state"],
                    "stars": data["stars"],
                    "review_count": data["review_count"]
                }

                # Avoid malformed quote fields by checking type & escaping ourselves
                if any(
                    isinstance(v, str) and ('\n' in v or '"' in v)
                    for v in row.values()
                ):
                    raise ValueError("Line contains risky characters")

                writer.writerow(row)

            except Exception as e:
                print(f"Skipping line {line_num}: {e}")
                continue

def convert_users_json_to_csv(json_file_path, csv_file_path):
    fieldnames = ["user_id", "name", "review_count", "yelping_since", "average_stars"]

    with open(json_file_path, 'r', encoding='utf-8') as infile, \
         open(csv_file_path, 'w', newline='', encoding='utf-8') as outfile:

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for line_num, line in enumerate(infile, 1):
            try:
                data = json.loads(line)
                writer.writerow({
                    "user_id": data.get("user_id"),
                    "name": data.get("name"),
                    "review_count": data.get("review_count"),
                    "yelping_since": data.get("yelping_since"),
                    "average_stars": data.get("average_stars")
                })
            except Exception as e:
                print(f"Line {line_num} skipped due to error: {e}")
                continue

def convert_reviews_json_to_csv(json_file_path, csv_file_path, user_ids_set, business_ids_set, max_lines=1000):
    fieldnames = ["review_id", "user_id", "business_id", "stars", "text", "date"]

    with open(json_file_path, 'r', encoding='utf-8') as infile, \
         open(csv_file_path, 'w', newline='', encoding='utf-8') as outfile:

        writer = csv.DictWriter(
            outfile,
            fieldnames=fieldnames,
            delimiter=',',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\'
        )
        writer.writeheader()

        for line_num, line in enumerate(infile, 1):
            if line_num > max_lines:
                break

            try:
                data = json.loads(line)

                user_id = data.get("user_id")
                business_id = data.get("business_id")

                # Skip if user_id or business_id not in original sets
                if user_id not in user_ids_set or business_id not in business_ids_set:
                    print(f"Skipping review {data.get('review_id')} at line {line_num} due to missing foreign key.")
                    continue

                clean_text = data.get("text", "").replace("\n", " ").replace("\r", " ")

                writer.writerow({
                    "review_id": data.get("review_id"),
                    "user_id": user_id,
                    "business_id": business_id,
                    "stars": data.get("stars"),
                    "text": clean_text,
                    "date": data.get("date")
                })

            except Exception as e:
                print(f"Skipping line {line_num}: {e}")
                continue



if __name__ == "__main__":

    convert_businesses_json_to_csv("yelp_academic_dataset_business.json", "businesses.csv")
    convert_users_json_to_csv("yelp_academic_dataset_user.json", "users.csv")

    users = pd.read_csv('users.csv')
    user_ids_set = set(users['user_id'].unique())

    businesses = pd.read_csv('businesses.csv')
    business_ids_set = set(businesses['business_id'].unique())

    convert_reviews_json_to_csv("yelp_academic_dataset_review.json", "reviews.csv", user_ids_set, business_ids_set, max_lines=100000)
