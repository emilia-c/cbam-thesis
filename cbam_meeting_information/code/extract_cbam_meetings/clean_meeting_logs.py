import json
import re

# helper function to clean text
def clean_text(text):
    if isinstance(text, str):
        text = re.sub(r'\s+', ' ', text).strip()  # remove extra whitespace and line breaks
    return text

# function to clean a meeting entry
def clean_meeting(meeting):
    if isinstance(meeting, dict):
        return {key: clean_text(value) for key, value in meeting.items()}
    elif isinstance(meeting, str):
        return clean_text(meeting)
    else:
        return meeting

# function to clean the entire dataset
def clean_data(data, term):
    for entry in data:
        entry["name"] = clean_text(entry.get("name", ""))
        entry["group"] = clean_text(entry.get("group", ""))
        entry["orgin_country"] = clean_text(entry.get("origin_country", ""))
        entry["national_party"] = clean_text(entry.get("national_party", ""))

        # clean meetings if it exists and is a dictionary
        meetings = entry.get("meetings", {})
        if isinstance(meetings, dict):
            for meeting_id, meeting in meetings.items():
                meetings[meeting_id] = clean_meeting(meeting)
        elif isinstance(meetings, str):
            # if meetings is a string like "No meetings for this MEP", just clean it
            entry["meetings"] = clean_text(meetings)

        # Add the term information
        entry["term"] = term

    return data

# file paths
path_9th_term = 'cbam_meeting_information/data/all_meetings/MEP_MEETINGS_9_TERM.json'
path_10th_term = 'cbam_meeting_information/data/all_meetings/MEP_MEETINGS_10_TERM.json'

# load and clean the 9th term data
with open(path_9th_term, 'r', encoding='utf-8') as file:
    data_9th_term = json.load(file)
    cleaned_9th_term = clean_data(data_9th_term, term=9)

# load and clean the 10th term data
with open(path_10th_term, 'r', encoding='utf-8') as file:
    data_10th_term = json.load(file)
    cleaned_10th_term = clean_data(data_10th_term, term=10)

# save the cleaned data to new JSON files
with open('cbam_meeting_information/data/all_meetings/cleaned_meetings_9term.json', 'w', encoding='utf-8') as file:
    json.dump(cleaned_9th_term, file, indent=4, ensure_ascii=False)

with open('cbam_meeting_information/data/all_meetings/cleaned_meetings_10term.json', 'w', encoding='utf-8') as file:
    json.dump(cleaned_10th_term, file, indent=4, ensure_ascii=False)

print("Data cleaned successfully and saved.")