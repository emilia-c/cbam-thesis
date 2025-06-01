import pandas as pd
import json
import os
from fuzzywuzzy import process, fuzz

# Define file paths
base_path = "cbam_meeting_information/data/cbam_specific_meetings"
ec_path = os.path.join(base_path, "ec/eu_commission_cbam_meetings.xlsx")
ep_term9_path = os.path.join(base_path, "ep/cbam_term9_meetings.json")
ep_term10_path = os.path.join(base_path, "ep/cbam_term10_meetings.json")
output_excel = "cbam_meeting_information/data/cbam_specific_meetings/ep_and_ec/RAW_merged_cbam_meetings.xlsx"
output_json = "cbam_meeting_information/data/cbam_specific_meetings/ep_and_ec/RAW_ep_ec_cbam_meetings.json"
registry_path = "cbam_meeting_information/data/transparency_registry/07.2024_registered_orgs_grouped.json"

###### 1. TRANSPARENCY REGISTER #########################
# Load transparency registry data
with open(registry_path, 'r', encoding='utf-8') as f:
    registry_data = json.load(f) 

# Create a lookup for organization details from the transparency registry.
# We store the registered name (the key itself), registration category, members, total budget, and headquarters country.
org_to_details = {
    org: {
        "registered_name": org,
        "Category": details[0].get("registration_category", "N/A"),
        "Members": details[0].get("members", "N/A"),
        "Budget": details[0].get("total_budget", "N/A"),
        "registered_country": details[0].get("hq_country", "N/A")
    }
    for org, details in registry_data.items()
}

# Define a function to get matching organization details using fuzzy matching.
def get_org_details(org_name, org_to_details, threshold=80):
    if not org_name:
        return {
            "org_match": "N/A", 
            "registered_name": "N/A", 
            "registered_country": "N/A",
            "Category": "N/A", 
            "Members": "N/A", 
            "Budget": "N/A"
        }
    best_match, score = process.extractOne(org_name, list(org_to_details.keys()), scorer=fuzz.token_sort_ratio)
    if score >= threshold:
        details = org_to_details[best_match]
        return {
            "org_match": best_match,
            "registered_name": details["registered_name"],
            "registered_country": details["registered_country"],
            "Category": details["Category"],
            "Members": details["Members"],
            "Budget": details["Budget"]
        }
    else:
        return {
            "org_match": "not_found", 
            "registered_name": "N/A", 
            "registered_country": "N/A",
            "Category": "N/A", 
            "Members": "N/A", 
            "Budget": "N/A"
        }
###### 2. EC MEETINGS #########################
# Load EC meetings from Excel
ec_df = pd.read_excel(ec_path)

# Drop unnecessary columns
ec_df = ec_df.drop(columns=["portfolio", "nr"], errors="ignore")

# Add "institution" column
ec_df["institution"] = "ec"

# Rename columns to match final structure
ec_df.rename(columns={"date": "date", "reason": "meeting_reason", "meeting_with": "meeting_with", "lobby_organisation": "org"}, inplace=True)

###### 3. EP MEETINGS #########################
# Load EP meetings from JSON files
def load_ep_meetings(json_path, term):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    # Add institution and term
    df["institution"] = "ep"
    df["term"] = term
    
    # Rename columns to match final structure
    df.rename(columns={"Date": "date", "Reason": "meeting_reason", "MEP": "meeting_with", "Meeting With": "org"}, inplace=True)

    # Return only required columns
    return df[["date", "meeting_reason", "meeting_with", "org", "institution", "term"]]

# Load EP term 9 and 10 meetings
ep_term9_df = load_ep_meetings(ep_term9_path, 9)
ep_term10_df = load_ep_meetings(ep_term10_path, 10)


#### 4. MERGE DATA ##############
# Concatenate all data into one DataFrame
final_df = pd.concat([ec_df, ep_term9_df, ep_term10_df], ignore_index=True)

#### 5. ADD ORG MATCHES FROM TRANSPARENCY REGISTRY ########
# Add new columns: org_category, org_budget, and org_member by performing fuzzy matching
def add_registry_info(row):
    details = get_org_details(row["org"], org_to_details)
    return pd.Series({
        "org_match": details["org_match"],
        "registered_category": details["Category"],
        "registered_member": details["Members"],
        "registered_budget": details["Budget"], 
        "registered_name": details["registered_name"],
        "registered_country": details["registered_country"]
    })

registry_info = final_df.apply(add_registry_info, axis=1)
final_df = pd.concat([final_df, registry_info], axis=1)

# Save to Excel
final_df.to_excel(output_excel, index=False)

# Save to JSON
final_df.to_json(output_json, orient="records", indent=4)

print("Merging complete! Files saved.")