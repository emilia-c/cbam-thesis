import os
import sqlite3
import random
import time
import aiohttp
import asyncio
from datetime import datetime

from stakeholder_data_extraction_pipeline import config
from .ddg_file_detection import detect_file_type # to detect file type

# Semaphore to limit concurrent downloads
semaphore = asyncio.Semaphore(5)  # adjust based on system/network capacity

async def download_file_and_update_status(conn, url_data, session):
    """ download a file asynchronously and update its status in db, url_data = (id, organization_id, url, file_path) """
    url_id, org_id, url, file_path = url_data  # unpack url data tuple
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_type, paywall_status = "unknown", "unknown"  # initialize variables

    async with semaphore: # control concurrency
        try:
            # make http request
            #async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                # if file is foribben to download or not found
                if response.status in [403, 404]: 
                    print(f"failed to download {url}: http {response.status}")   # print error status
                    await update_db(conn, url_id, f"failure_{response.status}", timestamp, file_type, paywall_status)  # update as failure 
                    return # exit 
                
                response.raise_for_status()  # raise error for other status codes (not 403 or 404)
                
                # if file can be downloaded, save file
                full_path = os.path.join(config.URL_DOWNLOADS_DIR, file_path)  # build full path
                os.makedirs(os.path.dirname(full_path), exist_ok=True)         # ensure directory exists
                with open(full_path, "wb") as f:
                    f.write(await response.read())  # write content to file in binary mode
        
            # after download, detect file type, paywall status and timestamp
            file_type, paywall_status = detect_file_type(full_path)
            #timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            await update_db(conn, url_id, "success", timestamp, file_type, paywall_status)  # if downloaded correctly, update db with success and other vars
        
        except aiohttp.ClientError as e:
            print(f"failed to download {url}: {e}")
            await update_db(conn, url_id, "failure", timestamp, file_type, paywall_status)

        except Exception as e:
            print(f"Unexpected error with {url}: {e}")
            await update_db(conn, url_id, "failure", timestamp, file_type, paywall_status)

        # add a random delay between download (to prevent overwhelming server)
        finally:
            # Random delay between downloads
            await asyncio.sleep(random.uniform(1, 8))

async def update_db(conn, url_id, download_status, timestamp, file_type, paywall_status):
    """ update urls table for a specific id, with retry mechanism for locked db """
    #conn = None # ensure conn exists
   
    try: 
        # connect to db
        #conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
        #c = conn.cursor()
        retries = 5 # number of retries (to avoid locked db due to multiple processes)
        
        for attempt in range(retries):
            try:
                # update url download status, add time stamp
                c = conn.cursor()
                c.execute("""
                UPDATE urls 
                SET download_status = ?, timestamp = ?, file_type = ?, paywall_status = ?
                WHERE id = ?
                """, (download_status or "unknown",         # ensure download_status is not None 
                      timestamp or "2025-01-01 00:00:00",   # default timestamp if None
                      file_type or "unknown",               # default file type if None
                      paywall_status or "unknown",          # default paywall_status if None
                      url_id))
                conn.commit()  # commit to db
                return         # exit function if successful
            
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    print(f"database locked, retrying... ({retries} retries left)")  # print retry message
                    #retries -= 1
                    await asyncio.sleep(20)  # wait 20 secs before retrying
                else: 
                    raise

    except sqlite3.Error as e:
        print(f"Error updating urls download status {e}")
    
    #finally: 
    #    if conn: 
    #        conn.close()  # close db connection

async def download_all_files():
    """ download all files with pending status sequentially, fetch pending urls and process each one """
    conn = None # ensure conn exists
    
    try: 
        conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT id, organization_id, url, file_path FROM urls WHERE download_status = "pending"')  # get pending downloads
        urls = c.fetchall()  # fetch all pending records
        
        # process each url and download
        #for url_data in urls:
        #    await download_file_and_update_status(url_data)  # process each url sequentially
        
        # process each URL asynchronously (overwhelmed servers so had to rewrite)
        #tasks = [download_file_and_update_status(conn, url_data) for url_data in urls]
        #await asyncio.gather(*tasks)
        
        timeout = aiohttp.ClientTimeout(total=30)  # 30-second timeout per request
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [download_file_and_update_status(conn, url_data, session) for url_data in urls]
            await asyncio.gather(*tasks)

    except sqlite3.Error as e:
        print(f"Error fetching pending urls {e}")
    
    finally: 
        if conn: 
            conn.close()  # close db connection

def run_downloader():
    """ run the async downloader, scheduling tasks, handles event loop issues"""
    
    # check for an active loop (to avoid crashing in jupyter notebook)
    try:
        loop = asyncio.get_event_loop()  # get current event loop
        
        # if a loop exists, schedule the downloads
        if loop.is_running():
            print("event loop already, scheduling downloads") # print message
            loop.create_task(download_all_files())            # schedule on current loop
        
        # run event loop
        else:
            print("running downloader with asyncio.run()")  # print message
            asyncio.run(download_all_files())               # run new event loop
    
    except Exception as e:
        print(f"error running downloader: {e}")  # print exception
