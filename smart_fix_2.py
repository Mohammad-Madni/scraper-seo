import os, csv, re, json, time
from base import Helper

# --- تنظیمات اختصاصی پوشه دوم ---
# BASE_FOLDER passed to Helper
# REPORT_CSV derived from Helper path
MIN_SIZE_KB = 2  # حساسیت روی فایل‌های زیر 2 کیلوبایت

DIRECTORY_DOMAINS = [
    'hipages.com.au', 'yelp.com', 'yelp.com.au', 'yellowpages.com.au', 
    'truelocal.com.au', 'facebook.com', 'instagram.com', 'starofservice.com.au',
    'checkatrade.com', 'buy.nsw.gov.au', 'localsearch.com.au'
]

class SmartFixer2(Helper):
    def __init__(self):
        super().__init__(base_output_folder="parsed_content_markdowns2")
        self.report_csv = os.path.join(self.base_output_folder, "_report_parsed_content_2.csv")

    def clean_target_url(self, url):
        """اصلاح یو‌ار‌ال مخصوص سایت Empire Roofing و حذف .php"""
        if "empireroofing.com.au" in url.lower():
            clean_url = re.sub(r'\.php$', '', url.strip())
            return clean_url
        return url.strip()

    def run_mega_fixer_v2_light(self):
        targets = []
        all_files_data = []
        
        if not os.path.exists(self.base_output_folder):
            print(f"❌ Error: Folder '{self.base_output_folder}' not found!")
            return

        print(f"🔍 Step 1: Scanning '{self.base_output_folder}' for files < {MIN_SIZE_KB}KB...")

        for root, dirs, files in os.walk(self.base_output_folder):
            for file in files:
                if file.endswith(".md") and not file.startswith("_"):
                    file_path = os.path.join(root, file)
                    file_size = os.path.getsize(file_path) / 1024 

                    if file_size < MIN_SIZE_KB:
                        try:
                            raw_url = ""
                            with open(file_path, 'r', encoding='utf-8') as f:
                                lines = f.readlines()
                                if len(lines) > 1:
                                    raw_url = lines[1].replace('# URL:', '').strip()
                            
                            if "http" in raw_url.lower():
                                final_url = self.clean_target_url(raw_url)
                                rg_match = re.search(r'_rg(\d+)', file)
                                r_grp = int(rg_match.group(1)) if rg_match else 0
                                is_directory = any(domain in final_url.lower() for domain in DIRECTORY_DOMAINS)
                                
                                # طبق خواسته شما: فقط 10 تای اول کریتیکال، بقیه Error
                                issue_type = "Error (Directory)" if is_directory else "CRITICAL (Top 10)"
                                if not is_directory and r_grp > 10:
                                    issue_type = f"Error (Low Rank: {r_grp})"

                                row = {
                                    'Issue': issue_type,
                                    'rank_group': r_grp,
                                    'url': final_url,
                                    'file_path': file_path
                                }
                                if issue_type == "CRITICAL (Top 10)":
                                    targets.append(row)
                        except: continue

        if not targets:
            print("🏁 No high-priority targets found."); return

        print(f"🚀 Step 2: Rescuing {len(targets)} sites with LIGHT settings (Switch Pool Only)...")
        
        import post_page
        import requests
        
        # Ensure smart_fix directory exists
        smart_fix_dir = "smart_fix"
        if not os.path.exists(smart_fix_dir):
            os.makedirs(smart_fix_dir)
        
        # headers available
        
        # Prepare batch
        tasks_bucket = []
        metadata_map = {}
        
        for item in targets:
            tag = item['file_path']
            url = item['url']
            # ⚡ Light Settings
            post_data = {
                "target": re.sub(r'(https?://|www\.)', '', url).split('/')[0], # domain
                "start_url": url,
                "url": url,
                "enable_content_parsing": True,
                "max_crawl_pages": 1,
                "enable_javascript": False,   
                "enable_browser_rendering": False,
                "enable_xhr": False,              
                "switch_pool": True,              
                "proxy_country": "AU",
                "tag": tag
            }
            tasks_bucket.append(post_data)
            metadata_map[tag] = item
            
        # Process batch
        batch_size = 100
        total_processed = 0

        for i in range(0, len(tasks_bucket), batch_size):
            batch = tasks_bucket[i:i + batch_size]
            print(f"📡 Posting batch {i//batch_size + 1} ({len(batch)} tasks)...")
            
            resp = post_page.post_onpage_task(self.headers, batch, output_dir=smart_fix_dir, filename=f"smart_fix_v2_batch_{i}.json")
            
            if not resp or resp.status_code != 200:
                 print(f"❌ Batch post failed for batch {i}")
                 continue
                 
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
                                with open(tag_path, 'w', encoding='utf-8') as f:
                                    json.dump(result_data, f, indent=4)
                                
                                new_size = os.path.getsize(tag_path) / 1024
                                print(f"   ✨ Success! New size: {new_size:.2f} KB")
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
    SmartFixer2().run_mega_fixer_v2_light()
