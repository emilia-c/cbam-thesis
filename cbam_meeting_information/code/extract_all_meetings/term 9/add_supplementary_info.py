import json

# Load the first JSON (Meetings Data)
with open('cbam_meeting_information/data/all_meetings/MEETINGS_9term.json', 'r', encoding='utf-8') as file:
    meetings_data = json.load(file)

# Load the second JSON (Supplementary Data)
with open('cbam_meeting_information/data/mep_list/9term_meps_all_info.json', 'r', encoding='utf-8') as file:
    supplementary_data = json.load(file)

# Create a dictionary from the supplementary data for quick lookup by name
supplementary_dict = {entry['name']: entry for entry in supplementary_data}

# Merge the data
merged_data = []

for meeting_entry in meetings_data:
    name = meeting_entry["name"]
    # Find the corresponding supplementary entry by name
    supplementary_entry = supplementary_dict.get(name, {})

    # Create a merged entry
    merged_entry = {
        "name": name,
        "group": supplementary_entry.get("group", ""),
        "origin_country": supplementary_entry.get("origin_country", ""),
        "national_party": supplementary_entry.get("national_party", ""),
        "meetings": meeting_entry.get("meetings", {})
    }

    # Add the merged entry to the final list
    merged_data.append(merged_entry)

with open('MEP_MEETINGS_9_TERM.json', 'w', encoding='utf-8') as file:
    json.dump(merged_data, file, indent=4, ensure_ascii=False)

print("Data merged successfully and saved to 'merged_data.json'.")
