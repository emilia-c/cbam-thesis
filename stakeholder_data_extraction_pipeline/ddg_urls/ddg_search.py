import pandas as pd
import os
import csv
import time
import random
import json

# import user configs
from stakeholder_data_extraction_pipeline import config 

# library to run ddg search 
from duckduckgo_search import DDGS 
from duckduckgo_search.exceptions import RatelimitException, TimeoutException, DuckDuckGoSearchException

def load_stakeholders():
    """ load stakeholder names and dataframe from excel """
    stake_df = pd.read_excel(config.STAKEHOLDER_FILE, 
                       sheet_name=config.STAKEHOLDER_SHEET)  # read excel file
    stakeholders = stake_df['search_title'].values.tolist()  # get searchable stakeholder names
    
    return stakeholders, stake_df  # return list and full dataframe

def load_existing_results():
    """ 
    Load existing DDG search results from CSV.
    Consolidate entries so that each organization maps to a list of URLs.
    """
    #existing_results = set()    # use set for fast lookup of successful searches
    #rate_limited_orgs = set()   # set tracking for rate-limited failures
    
    results_dict = {}  # to store the latest result for each organization
    csv_file = config.LOGS_DIR + "/ddg_search_results.csv"
    
    try:
        with open(csv_file, "r", encoding="utf-8", errors="replace") as file:
            reader = csv.reader(file)
            header = next(reader, None)  # skip header if present
            for row in reader:
                if len(row) < 2:
                    continue
                org, url = row[0], row[1]
                if org not in results_dict:
                    results_dict[org] = []      # initialize list for each org
                results_dict[org].append(url)   # append each URL
    except FileNotFoundError:
        print("CSV file not found, running full search")
    
    existing_results = set()
    rate_limited_orgs = set()
    error_threshold = 3

    for org, urls in results_dict.items():
        error_count = sum(1 for url in urls if url in ["rate_limited", "error"])
        if error_count >= error_threshold:
            # If error count is too high, mark org as timed_out.
            rate_limited_orgs.add(org)
            # Also, update the results_dict so that this org shows only "timed_out"
            results_dict[org] = ["timed_out"]
        elif any(url not in ["rate_limited", "error", "timed_out"] for url in urls):
            existing_results.add(org)
        else:
            rate_limited_orgs.add(org)
    
    # write the consolidated results back to the CSV to remove duplicates
    with open(csv_file, "w", newline="", encoding="utf-8", errors="replace") as file:
        writer = csv.writer(file)
        writer.writerow(["organisation", "url"])
        for org, urls in results_dict.items():
            for url in urls:                    # write one row per url 
                writer.writerow([org, url])

    return existing_results, rate_limited_orgs

def perform_search(stakeholder_names):
    """ perform ddg search for each stakeholder """
    
    all_results = [] # where to store results
    
    with DDGS() as ddgs:
        for org in stakeholder_names:
            # build queries
            #if " " in org: 
            #    search_query = f'"{org}" {config.SEARCH_QUERY}' # build query for org names with whitespace
            #    print(f"searching for: {search_query}")  
            #else: 
            #    search_query = f'{org} {config.SEARCH_QUERY}'   # build query for org names w/out whitespace
            #    print(f"searching for: {search_query}")

            search_query = f'"{org}" {config.SEARCH_QUERY}'
           
            # try to avoid hitting rate limit
            #retries = 0
            #max_retries = 3
            #success = False
            org_results = []  

            try:
                results = ddgs.text(
                    search_query,                   # perform search
                    max_results=config.MAX_RESULTS, # set max results 
                    backend='auto'                  # use auto backend to avoid ratelimiting
                )
                
                if results:
                    for result in results:
                        org_results.append({"org": org, "url": result.get("href", "no_urls")})
                else:
                    org_results.append({"org": org, "url": "no_results"})
            
            except (RatelimitException, TimeoutException, DuckDuckGoSearchException, Exception) as e:
                print(f"Exception for {org}: {e}. Sleeping for 80 seconds and moving to next org.")
                time.sleep(80)  # easiest way to handle the DDGs backend error limit
                # mark this org as having an error for this run.
                # IMPORTANT !!!! this also marks urls even with partial success with an error and they will be re-run !!!!!
                # To do: make this more efficient
                org_results = [{"org": org, "url": "error"}]
            
            all_results.extend(org_results)
            save_search_results(org_results)  # Save results immediately after each org.
            
            # reload rate-limited orgs (MAYBE NOT NEEDED)
            _, _ = load_existing_results()
            
            time.sleep(random.uniform(1, 5))  # random sleep between requests
    return all_results
            
#            while not success and retries < max_retries:
                # extract urls
#                try:
#                    results = ddgs.text(search_query,                   # perform search
#                                        max_results=config.MAX_RESULTS, # set max results in config
#                                        backend='auto')                 # set backend to auto, avoid ratelimiting
                    
                    # if results, add url, if returns None log as no results
#                    if results: 
#                        for result in results: 
#                            org_results.append({"org": org, "url": result.get("href", "no_urls")})
                            #search_result = {"org": org, "url": results[0].get("href", "no_urls")}
#                    else: 
                        #search_result = {"org": org, "url": "no_results"}
#                        org_results.append({"org": org, "url": "no_results"})

#                    success = True  # exit the retry loop if successful
                    
#                except (RatelimitException, TimeoutException, DuckDuckGoSearchException, Exception) as e:
#                    print(f"Exception for {org}: {e}, retrying ({retries+1}/{max_retries})")
#                    retries += 1
#                    time.sleep(80)  # long wait on rate limit
                
            # if maximum retries reached without success, mark as timed_out (then inspect manually)
#            if not success:
#                org_results = [{"org": org, "url": "timed_out"}]
            
           
#            all_results.extend(org_results)    # add search result to all_results
#            save_search_results(org_results)   # save results immediately after each search

            # **reload rate-limited orgs** to avoid retrying successfully processed ones
#            _, rate_limited_orgs = load_existing_results()

#            time.sleep(random.uniform(1, 5))  # random sleep between requests
#    return all_results  # Return list of search results

#def remove_duplicates(results, existing_results):
#    """Ensure we don't add duplicate results to the CSV."""
#    unique_results = [res for res in results if res["org"] not in existing_results]
#    return unique_results

def save_search_results(results):
    """For logging and future reference, save ddg search results to csv """ 
    
    csv_file = config.LOGS_DIR + "/ddg_search_results.csv"  # where to save
    file_exists = os.path.isfile(csv_file)                  # check if file exists
    write_header = not file_exists                          # assume need header if no file

    if file_exists: 
        with open(csv_file, "r", encoding="utf-8", errors='replace') as file: 
            if file.read().strip() == "": 
                write_header = True # if file is empty, write header

    with open(csv_file, "a", newline="", encoding="utf-8", errors='replace') as file:
        writer = csv.writer(file)                           # write csv
        if write_header: 
            writer.writerow(["organisation", "url"])        # write header if file is new or empty
        for res in results:
            writer.writerow([res["org"], res["url"]])       # write row
    
    print(f"saved search results for {results[0]['org']} to {csv_file}")            # confirmation

def run_search_pipeline():
    """Run DDG search pipeline:
       - If no CSV exists (first run), search for all stakeholders.
       - If CSV exists, retry only those orgs that were rate-limited or missing.
       Save the search results to CSV.
    """
    stakeholders, stake_df = load_stakeholders()  # Load stakeholders from Excel
    csv_path = os.path.join(config.LOGS_DIR, "ddg_search_results.csv")

    # Load existing search results from CSV
    existing_results, rate_limited_orgs = load_existing_results()

    # Identify missing stakeholders (those in the Excel list but not in the existing results)
    missing_stakeholders = [stakeholder for stakeholder in stakeholders if stakeholder not in existing_results]
    results_list = []

    if not os.path.exists(csv_path):  # If no CSV exists, perform search for all stakeholders
        print("CSV not found. Running full search for all stakeholders.")
        results_list.extend(perform_search(stakeholders))
        
        #results = perform_search(stakeholders)
        #results = remove_duplicates(results, existing_results) # remove duplicate urls
        #save_search_results(results)

    else:
        print("CSV found. Checking if any stakeholders missing...")
        
        # if there are missing stakeholders, perform the search for those
        if missing_stakeholders:
            print(f"Performing search for missing stakeholders: {missing_stakeholders}")
            results_list.extend(perform_search(missing_stakeholders))
            #results = perform_search(missing_stakeholders)
            #results = remove_duplicates(results, existing_results)
            #save_search_results(results)

        # if there are rate-limited organizations, retry searching for those
        if rate_limited_orgs:
            print(f"Retrying rate-limited orgs: {list(rate_limited_orgs)}")
            results_list.extend(perform_search(list(rate_limited_orgs)))
            
            #results = perform_search(list(rate_limited_orgs))
            #results = remove_duplicates(results, existing_results)
            #save_search_results(results)
        
        # if all stakeholders are found, simply load results from the CSV
        if not missing_stakeholders and not rate_limited_orgs:
            print("All stakeholders are accounted for. Loading results from CSV.")

            with open(csv_path, "r", encoding="utf-8", errors="replace") as file:
                reader = csv.reader(file)
                next(reader, None)  # skip header
                results_list = [{"org": row[0], "url": row[1]} for row in reader]

    # convert results_list to JSON format
    ddg_results = [{"org": res["org"], "url": res["url"]} for res in results_list]

    # save results for debugging
    #with open("ddg_results_debug.json", "w", encoding="utf-8") as f:
    #    json.dump(ddg_results, f, indent=4)

    return ddg_results, stake_df # return results (used to update db) and stakeholder dataframe (used for org metadata for db)