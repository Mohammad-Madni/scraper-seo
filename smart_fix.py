import os, csv, re, json, time
from base import Helper

# --- تنظیمات ---
# BASE_FOLDER logic moves to Helper default or init arg
MIN_SIZE_KB = 10 

DIRECTORY_DOMAINS = [
    'hipages.com.au', 'yelp.com', 'yelp.com.au', 'yellowpages.com.au', 
    'truelocal.com.au', 'facebook.com', 'instagram.com', 'starofservice.com.au',
    'checkatrade.com', 'buy.nsw.gov.au', 'localsearch.com.au', 'au.nextdoor.com'
]

class SmartFixer(Helper):
    def __init__(self):
        super().__init__(base_output_folder="parsed_content_markdowns")
        self.report_csv = os.path.join(self.base_output_folder, "_final_scan_report.csv")

    def clean_target_url(self, url):
        """اصلاح یو‌ار‌ال مخصوص سایت Empire Roofing و حذف .php"""
        if "empireroofing.com.au" in url.lower():
            # حذف .php از انتهای آدرس در صورت وجود
            clean_url = re.sub(r'\.php$', '', url.strip())
            return clean_url
        return url.strip()

    def run_mega_fixer(self):
        targets = []
        print(f"🔍 Step 1: Scanning and Prioritizing (Top 10 + URL Cleaning)...")
        
        all_files_data = []
        for root, dirs, files in os.walk(self.base_output_folder):
            if "organic" not in root.lower(): continue

            for file in files:
                if file.endswith(".md") and not file.startswith("_"):
                    file_path = os.path.join(root, file)
                    file_size = os.path.getsize(file_path) / 1024 

                    if file_size < MIN_SIZE_KB:
                        try:
                            raw_url = ""
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                                raw_url = data.get('data', {}).get('start_url')
                            
                            if "http" in raw_url.lower():
                                # ⚡ اعمال اصلاح آدرس (حذف .php)
                                final_url = self.clean_target_url(raw_url)
                                
                                rg_match = re.search(r'_rg(\d+)', file)
                                r_grp = int(rg_match.group(1)) if rg_match else 0
                                is_directory = any(domain in final_url.lower() for domain in DIRECTORY_DOMAINS)
                                
                                issue_type = "Error (Directory)" if is_directory else "CRITICAL (Top 10)"
                                if not is_directory and r_grp > 10:
                                    issue_type = f"Error (Low Rank: {r_grp})"

                                row = {
                                    'Issue': issue_type,
                                    'suburb': os.path.basename(os.path.dirname(os.path.dirname(file_path))),
                                    'rank_group': r_grp,
                                    'url': final_url,
                                    'actual_size': f"{file_size:.2f} KB",
                                    'status': 'Skipped' if (is_directory or r_grp > 10) else 'Pending',
                                    'file_path': file_path
                                }
                                all_files_data.append(row)
                                
                                if issue_type == "CRITICAL (Top 10)":
                                    targets.append(row)
                        except: continue

        # ذخیره در CSV
        with open(self.report_csv, mode='w', newline='', encoding='utf-8') as csvfile:
            fields = ['Issue', 'suburb', 'rank_group', 'url', 'actual_size', 'status', 'file_path']
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(all_files_data)

        print(f"✅ Step 1 Done. Cleaned URLs and found {len(targets)} Top 10 targets.")

        if not targets:
            print("🏁 No high-priority targets to rescue."); return
            
        import post_page
        import requests
        
        # Ensure smart_fix directory exists
        smart_fix_dir = "smart_fix"
        if not os.path.exists(smart_fix_dir):
            os.makedirs(smart_fix_dir)
            
        # headers are available via self.headers from Helper
        
        # Prepare batch
        tasks_bucket = []
        metadata_map = {}
        
        for item in targets:
            # tag is file_path for identification
            tag = item['file_path']
            url = item['url']
            
            post_data = {
                "target": re.sub(r'(https?://|www\.)', '', url).split('/')[0], # domain
                "start_url": url,
                "url": url,
                "enable_content_parsing": True,
                "max_crawl_pages": 1,
                "enable_javascript": True,
                "load_resources": True,
                "enable_browser_rendering": True,
                "enable_xhr": True,
                "disable_cookie_popup": True,
                "browser_preset": "desktop",
                "proxy_country": "AU",
                "use_advanced_anti_robot_protection": True,
                "browser_wait_until": "fully_loaded",
                "wait_for_content_timeout": 30,
                "tag": tag
            }
            tasks_bucket.append(post_data)
            metadata_map[tag] = item
            
        # Process in batches
        batch_size = 100
        total_processed = 0
        
        for i in range(0, len(tasks_bucket), batch_size):
            batch = tasks_bucket[i:i + batch_size]
            print(f"📡 Posting batch {i//batch_size + 1} ({len(batch)} tasks)...")
            
            # Save batch to smart_fix directory
            resp = post_page.post_onpage_task(self.headers, batch, output_dir=smart_fix_dir, filename=f"smart_fix_batch_{i}.json")
            
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

            # Wait + Fetch Logic (Replacing Poll)
            fetch_payload = []
            for tid in task_ids:
                 tag = id_to_tag.get(tid)
                 meta = metadata_map.get(tag)
                 # url is required in payload for content_parsing endpoint usually? 
                 # In error-critical we used meta['target'] or meta['url']. 
                 # on_page_get uses 'url' field which corresponds to task 'data'->'start_url' usually.
                 if meta:
                     fetch_url = meta.get('url')
                     if fetch_url:
                        fetch_payload.append({"id": tid, "url": fetch_url})

            print(f"⏳ Waiting 2 minutes for results (Batch {i//batch_size + 1})...")
            time.sleep(120)
            
            print(f"📥 Fetching results for {len(fetch_payload)} tasks...")
            
            endpoint = "https://api.dataforseo.com/v3/on_page/content_parsing"
            try:
                fetch_resp = requests.post(endpoint, headers=self.headers, json=fetch_payload, timeout=120)
                
                if fetch_resp.status_code == 200:
                    res_json = fetch_resp.json()
                    for task_res in res_json.get("tasks", []):
                        # Logic to save
                        result_data = task_res
                        
                        tag_path = task_res.get("data", {}).get("tag")
                        
                        if not tag_path:
                             tid = task_res.get("id")
                             tag_path = id_to_tag.get(tid)

                        if not tag_path: continue
                        
                        item = metadata_map.get(tag_path)

                        # Save logic
                        status_msg = result_data.get('status_message')
                        if status_msg == 'Ok.':
                            try:
                                #@DEV make valid json dumps
                                with open(tag_path, 'w', encoding='utf-8') as f:
                                    json.dump(result_data, f, indent=4)
                                print(f"   ✨ Success! New size: {os.path.getsize(tag_path)/1024:.2f} KB")
                                total_processed += 1
                            except Exception as e:
                                 print(f"   💥 Save Error {tag_path}: {e}")
                        else:
                             if item:
                                print(f"   ❌ API Error for {item['url']}: {status_msg}")
                else:
                    print(f"❌ Fetch API Error: {fetch_resp.status_code}")
            except Exception as e:
                print(f"❌ Fetch Connection Error: {e}")

        print(f"🏁 Finished. Rescued {total_processed}/{len(targets)} files.")

if __name__ == "__main__":
    SmartFixer().run_mega_fixer()



#@ DEV make this accept a flag to only get tasks, inqueu
# @Dev
#         "start_url": "https://au.nextdoor.com/pages/inveria-roofing-abbotsford-nsw/",
#  as au.md
# yelp.com it strips this as m.md
