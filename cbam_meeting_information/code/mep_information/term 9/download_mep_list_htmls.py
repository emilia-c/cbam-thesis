import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
from wayback import WaybackClient
import random

# OBS: Might need to run a few times to get all of the archived snapshots, the wayback machine ratelimits easily

# set up constants
base_url = "https://www.europarl.europa.eu/meps/en/full-list/all"
start_date_str = "20190717"
end_date_str = "20240715"
cache_dir = "cbam_meeting_information/code/mep_information/term 9/cache"  # directory to store cached snapshots
#meps_list = []

# ensure cache directory exists
os.makedirs(cache_dir, exist_ok=True)

async def get_meps_from_snapshot(session, snapshot_url, snapshot_date_str):
    """Get MEP names, IDs, and date from a specific snapshot asynchronously."""
    try:
        cache_file = os.path.join(cache_dir, f"{snapshot_date_str}.html")

        # check if the data is cached
        if os.path.exists(cache_file):
            print(f"Loading data from cache for {snapshot_date_str}")
            return # skip downloading
        
        # fetch data if not cached
        async with session.get(snapshot_url) as response:
            response.raise_for_status()
            text = await response.text()

            # cache the fetched HTML content
            async with aiofiles.open(cache_file, 'w', encoding='utf-8') as f:
                await f.write(text)
        print(f"snapshot saved for {snapshot_date_str}")

        # Parse the HTML content
        #soup = BeautifulSoup(text, 'html.parser')
        #for mep in soup.find_all("a", class_="ep_content"):
        #    mep_name = mep["title"].strip()
        #    mep_href = mep["href"]
        #    mep_id = mep_href.split('/')[-1]

        #    meps_list.append({
        #        "mep_id": mep_id,
        #        "mep_name": mep_name,
        #        "date_scraped": snapshot_date_str
        #    })
        #await save_to_json(meps_list)

    except Exception as e:
        print(f"Error fetching data from {snapshot_url}: {e}")

#async def save_to_json(meps_list):
#    """Save the MEP list to a JSON file asynchronously."""
#    json_filename = "MEPs_2019_2024.json"
#    try:
#        async with aiofiles.open(json_filename, 'w', encoding='utf-8') as json_file:
#            await json_file.write(json.dumps(meps_list, ensure_ascii=False, indent=4))
#    except Exception as e:
#        print(f"Error saving data to JSON: {e}")

def get_wayback_snapshots(base_url, from_date, to_date):
    """Fetch all snapshots from Wayback Machine for the given URL within a date range."""
    client = WaybackClient()
    snapshots = {}

    from_date_dt = datetime.strptime(from_date, "%Y%m%d")
    to_date_dt = datetime.strptime(to_date, "%Y%m%d")

    try:
        print(f"Fetching snapshotsfrom {from_date_dt} to {to_date_dt}...")
        results = list(client.search(base_url, from_date=from_date_dt, to_date=to_date_dt))
        print(f"Total snapshots found: {len(results)}")

        for result in results:
            snapshot_date_str = result.timestamp.strftime('%Y-%m-%d')
            if snapshot_date_str not in snapshots:  # keep only one snapshot per day
                snapshots[snapshot_date_str] = result.raw_url
    
    except Exception as e:
        print(f"Error fetching snapshots for date range {from_date} to {to_date}: {e}")

    return snapshots

async def main():
    snapshots = get_wayback_snapshots(base_url, start_date_str, end_date_str)
    
    if not snapshots:
            print("no snapshots found, exiting")
            return

    async with aiohttp.ClientSession() as session:
        tasks = []
        for snapshot_date_str, snapshot_url in snapshots.items():
            cache_file = os.path.join(cache_dir, f"{snapshot_date_str}.html")

            # skip already cached snapshots
            if os.path.exists(cache_file):
                print(f"skipping cached snapshot for {snapshot_date_str}")
                continue

            snapshot_url = snapshot_url.replace("http://", "https://")  # enforce https
            print(f"queueing snapshot fetch: {snapshot_url}")

            task = get_meps_from_snapshot(session, snapshot_url, snapshot_date_str)
            tasks.append(task)

            # add random delay to avoid rate limiting
            await asyncio.sleep(random.uniform(1, 3))

        # run all downloads concurrently
        if tasks:
            await asyncio.gather(*tasks)
        else:
            print("All snapshots are already cached, no downloads needed")

# run the script
asyncio.run(main())
