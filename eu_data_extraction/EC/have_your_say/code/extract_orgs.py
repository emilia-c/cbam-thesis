import json
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

# Load feedback JSON
with open("eu_data_extraction/EC/have_your_say/data/combined_hys.json", "r", encoding="utf-8") as f:
    feedback_data = json.load(f)

# Extract relevant fields from feedback data
feedback_list = []
for item in feedback_data:
    feedback_list.append({
        "organization": item.get("organization"),
        "tr_number": item.get("tr_number"),
        "org_type": item.get("org_type")
    })

feedback_df = pd.DataFrame(feedback_list)

# Load transparency registry data
with open("cbam_meeting_information/data/transparency_registry/07.2024_registered_orgs_grouped.json", "r", encoding="utf-8") as f:
    registry_data = json.load(f)

# Build registry map by transparency number
registry_by_tr = {}
org_name_list = []  # For fuzzy matching

for org_name, entries in registry_data.items():
    org_name_list.append(org_name)
    for entry in entries:
        tr_number = entry.get("transparency_no")
        registry_by_tr[tr_number] = {
            "official_registered_name": org_name,
            "acronym": entry.get("acronym"),
            "members": entry.get("members"),
            "category":entry.get("registration_category"),
            "total_budget": entry.get("total_budget")
        }

# Enrich each row
def enrich_row(row):
    # Default empty values
    row["official_registered_name"] = None
    row["category"] = None
    row["acronym"] = None
    row["members"] = None
    row["total_budget"] = None

    tr_number = row.get("tr_number")
    if tr_number and tr_number in registry_by_tr:
        match = registry_by_tr[tr_number]
    else:
        # Try fuzzy match on organization name
        org_name = row.get("organization")
        if org_name:
            best_match, score = process.extractOne(org_name, org_name_list, scorer=fuzz.token_sort_ratio)
            if score >= 90:  # You can lower threshold if you want more matches
                # Take the first entry for this org name
                matched_entry = registry_data[best_match][0]
                match = {
                    "official_registered_name": best_match,
                    "category": matched_entry.get("registration_category"),
                    "acronym": matched_entry.get("acronym"),
                    "members": matched_entry.get("members"),
                    "total_budget": matched_entry.get("total_budget")
                }
            else:
                match = None
        else:
            match = None

    if match:
        row.update(match)
    return row

# Apply enrichment
enriched_df = feedback_df.apply(enrich_row, axis=1)

# Save to Excel
enriched_df.to_excel("eu_data_extraction/EC/have_your_say/data/all_hys_orgs.xlsx", index=False)