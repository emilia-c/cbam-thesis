# Import needed libraries
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import json

# XML URL for transparency registry
xml_url = "https://data.europa.eu/euodp/en/data/storage/f/2024-07-03T094150/ODP_30-06-2024.xml"

# namespace for the XML file found at <metaData xmlns="">
namespace = {'ns': ''}

# function to parse XML for variables of interest
def xml_to_df(root):
    data = []

    for entity in root.findall('.//ns:interestRepresentative', namespace):
        # parse XML for variables of interest

        # identification variables
        identification_code = entity.findtext('ns:identificationCode', default="N/A", namespaces=namespace)
        registration_date = entity.findtext('ns:registrationDate', default="N/A", namespaces=namespace)
        category_of_registration = entity.findtext('ns:registrationCategory', default="N/A", namespaces=namespace)
        name = entity.find('.//ns:name/ns:originalName', namespace).text if entity.find('.//ns:name/ns:originalName', namespace) is not None else "N/A"
        acronym = entity.findtext('ns:acronym', default="N/A", namespaces=namespace)
        head_office_country = entity.findtext('.//ns:headOffice/ns:country', default="N/A", namespaces=namespace)
        
        # stated interests
        interest = entity.find('.//ns:interestRepresented', namespace).text if entity.find('.//ns:interestRepresented', namespace) is not None else "N/A"
        mission_statement = entity.findtext('ns:goals', default="N/A", namespaces=namespace)
        
        # how many active lobbyists
        members = entity.find('.//ns:members/ns:members', namespace).text if entity.find('.//ns:members/ns:members', namespace) is not None else "N/A"
        
        # website 
        website = entity.findtext('ns:webSiteURL', default="N/A", namespaces=namespace)
        
        # total budget from closed year
        total_budget_element = entity.find('.//ns:financialData/ns:closedYear/ns:totalBudget/ns:absoluteCost', namespace)
        total_budget = total_budget_element.text if total_budget_element is not None else "N/A"

        # add found data to the list
        data.append({
            'transparency_no': identification_code,
            'reg_date': registration_date,
            'name': name,
            'registration_category': category_of_registration, 
            'acronym': acronym,
            'hq_country': head_office_country,
            'website': website, 
            'interest_level': interest,
            'mission': mission_statement, 
            'members': members,
            'total_budget': total_budget
        })

    return pd.DataFrame(data)

# fetch transparency register data
response = requests.get(xml_url)
response.raise_for_status()
xml_content = response.content

# parse XML data from response content
root = ET.fromstring(xml_content)

# read XML into DataFrame using xml_to_df
t_reg = xml_to_df(root)

# preview DataFrame
print(t_reg.head())
print(t_reg.tail())

# clean the registration names
t_reg['name'] = t_reg['name'].str.replace(r'\n+', ' ', regex=True).str.strip()

# group by the 'name' field and convert to a dictionary
grouped_data = t_reg.groupby('name').apply(lambda x: x.drop('name', axis=1).to_dict(orient='records')).to_dict()

# save the grouped data as a JSON file
with open('07.2024_registered_orgs_grouped.json', 'w', encoding='utf-8') as json_file:
    json.dump(grouped_data, json_file, ensure_ascii=False, indent=4)

# Save the unique DataFrame to an Excel file
t_reg.to_excel('07.2024_registered_orgs.xlsx', index=False, engine='openpyxl')