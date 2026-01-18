import csv
import json
import requests
import base64
import os
import re
import shutil
from config import USERNAME, PASSWORD
import post_page
import time

def retry_organic_critical_and_errors(base_folder="parsed_content_markdowns", new_folder="parsed_content_markdowns2"):
    # ۱. آماده‌سازی پوشه مقصد
    if not os.path.exists(new_folder):
        print(f"📂 Creating {new_folder}...")
        os.makedirs(new_folder)
    
    auth_str = f"{USERNAME}:{PASSWORD}"
    token = base64.b64encode(auth_str.encode()).decode()
    headers = {'Authorization': f'Basic {token}', 'Content-Type': 'application/json'}

    summary_csv_path = os.path.join(base_folder, "_error_summary.csv")
    new_summary_path = os.path.join(new_folder, "_retry_organic_report.csv")
    
    if not os.path.exists(summary_csv_path):
        print(f"❌ Error: {summary_csv_path} not found!")
        return

    summary_fields = ['Issue', 'suburb', 'service', 'type', 'rank', 'rank_group', 'url', 'error_type', 'status']
    
    with open(new_summary_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()

    processed_count = 0 
    skipped_count = 0
    
    tasks_bucket = []
    metadata_map = {} # Map ID (or temp index) to file metadata

    print("🚀 Collecting tasks for retry...")

    with open(summary_csv_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # --- فیلتر اصلاح شده: (CRITICAL یا ERROR یا PENDING) + (ORGANIC) + (RankGroup 1-5) ---
            issue_val = str(row.get('Issue', '')).upper().strip()
            type_val = str(row.get('type', '')).lower().strip()
            status_val = str(row.get('status', '')).lower().strip()
            rank_gp = row.get('rank_group', '0')
            
            # Check Rank Group (1-5)
            try:
                rg_int = int(rank_gp)
            except:
                rg_int = 100 # default high to skip

            if rg_int < 1 or rg_int > 5:
                 skipped_count += 1
                 continue

            if "organic" not in type_val:
                 skipped_count += 1
                 continue

            # Check Issue or Status
            # User wants: "status was pending" OR Critical/Error
            is_target = False
            if issue_val in ["CRITICAL", "ERROR"]:
                is_target = True
            elif "pending" in status_val or "pending" in issue_val.lower():
                is_target = True
            
            if not is_target:
                skipped_count += 1
                continue 
            
            # -------------------------------------------------------

            suburb = row.get('suburb', 'Unknown')
            service = row.get('service', 'service')
            item_type = type_val.replace(' ', '_')
            rank_abs = row.get('rank', '0')
            url = row.get('url', '')
            
            if not url or "google.com" in url or not url.startswith("http"):
                continue

            suburb_slug = str(suburb).strip().replace(' ', '-')
            target_path = os.path.join(new_folder, suburb_slug, item_type)
            os.makedirs(target_path, exist_ok=True)

            domain_match = re.sub(r'(https?://|www\.)', '', url).split('/')[0].split('.')[0]
            filename = f"type-{item_type}_rg{rank_gp}_ra{rank_abs}_{domain_match}.md"
            file_path = os.path.join(target_path, filename)
            
            # Prepare data
            domain_match = re.sub(r'(https?://|www\.)', '', url).split('/')[0].split('.')[0]
            
            task_payload = {
                "target": re.sub(r'(https?://|www\.)', '', url).split('/')[0], # domain
                "start_url": url,
                "url": url, # Keep url for compatibility if needed, but start_url is standard for crawl
                "enable_content_parsing": True,
                "max_crawl_pages": 1,
                "enable_javascript": True,
                "enable_browser_rendering": True,
                "load_resources": True,
                "disable_cookie_popup": True,
                "browser_wait_until": "fully_loaded",
                # "internal_content_analysis": True, # User didn't explicitly ask for this but good to keep if valid
                # Store metadata in tag for retrieval
                "tag": file_path 
            }
            
            tasks_bucket.append(task_payload)
            
            # We also need these for reporting error failure if needed, 
            # but tag is usually enough to identify the file.
            metadata_map[file_path] = {
                "issue_val": issue_val,
                "suburb": suburb,
                "service": service,
                "type": item_type,
                "rank": rank_abs,
                "rank_group": rank_gp,
                "url": url
            }
            
            print(f"➕ Queued: {domain_match}")

    # Process in batches of 100
    batch_size = 100
    total_posted = 0
    
    for i in range(0, len(tasks_bucket), batch_size):
        batch = tasks_bucket[i:i + batch_size]
        print(f"📡 Posting batch {i//batch_size + 1} ({len(batch)} tasks)...")
        
        # Post
        resp = post_page.post_onpage_task(headers, batch, output_dir=new_folder, filename=f"retry_batch_{i}.json")
        
        if not resp or resp.status_code != 200:
             print(f"❌ Batch post failed for batch {i}")
             continue
             
        # Extract IDs
        resp_data = resp.json()
        task_ids = []
        id_to_tag = {}
        
        for task in resp_data.get('tasks', []):
            tid = task.get('id')
            tag = task.get('data', {}).get('tag')
            if tid and tag:
                task_ids.append(tid)
                id_to_tag[tid] = tag

        total_posted += len(task_ids)
        total_posted += len(task_ids)
        
        # Prepare payload for fetching results
        fetch_payload = []
        for tid in task_ids:
             # We need ID and URL (from metadata)
             tag = id_to_tag.get(tid)
             meta = metadata_map.get(tag)
             if meta:
                 fetch_payload.append({"id": tid, "url": meta.get('target', meta.get('url'))}) # target preferred if available? on_page_get uses url/start_url

        print(f"⏳ Waiting 2 minutes for results (Batch {i//batch_size + 1})...")
        time.sleep(120) 
        
        print(f"📥 Fetching results for {len(fetch_payload)} tasks...")
        
        # Copied/Adapted from on_page_get.py
        endpoint = "https://api.dataforseo.com/v3/on_page/content_parsing"
        try:
            fetch_resp = requests.post(endpoint, headers=headers, json=fetch_payload, timeout=120)
            
            if fetch_resp.status_code == 200:
                res_json = fetch_resp.json()
                for task_res in res_json.get("tasks", []):
                    # Logic to save
                    result_data = task_res
                    
                    # Original logic uses tag from the result task_res
                    # task_res['data']['tag'] should remain if API preserved it
                    tag_path = task_res.get("data", {}).get("tag")
                    
                    if not tag_path:
                         # Fallback if tag lost? usually preserved.
                         # map ID to tag?
                         tid = task_res.get("id")
                         tag_path = id_to_tag.get(tid)

                    if not tag_path: continue

                    # Save logic
                    status_msg = result_data.get('status_message')
                    if status_msg == 'Ok.':
                        try:
                            with open(tag_path, "w", encoding="utf-8") as f:
                                json.dump(result_data, f, indent=4)
                            print(f"✅ Saved: {os.path.basename(tag_path)}")
                            processed_count += 1
                        except Exception as e:
                             print(f"❌ Save Error {tag_path}: {e}")
                    else:
                         # Log failure
                         # Need metadata for logging
                         meta = metadata_map.get(tag_path, {})
                         with open(new_summary_path, 'a', newline='', encoding='utf-8') as f_err:
                             writer = csv.DictWriter(f_err, fieldnames=summary_fields)
                             writer.writerow({
                                'Issue': f"RETRY_FAILED_{meta.get('issue_val', 'Unknown')}",
                                'suburb': meta.get('suburb'),
                                'service': meta.get('service'),
                                'type': meta.get('type'),
                                'rank': meta.get('rank'),
                                'rank_group': meta.get('rank_group'),
                                'url': meta.get('url'),
                                'error_type': 'api_error',
                                'status': status_msg
                             })
            else:
                 print(f"❌ Fetch API Error: {fetch_resp.status_code}")

        except Exception as e:
            print(f"❌ Fetch Connection Error: {e}")

        # Clean loop variables or continues...
        # Next batch loop
    print("\n" + "="*40)
    print(f"✨ Task Finished!")
    print(f"✅ Total Queued: {len(tasks_bucket)}")
    print(f"✅ Processed/Saved: {processed_count}")
    print(f"⏭️ Total Skipped (Filter): {skipped_count}")
    print(f"📊 New Report: {new_summary_path}")
    print("="*40)

if __name__ == "__main__":
    retry_organic_critical_and_errors()
