import os
import csv
import json
import requests
import base64
from config import USERNAME, PASSWORD
from base import Helper, CsvColumn
import time


class ResultFetcher(Helper):
    def __init__(self):
        # Queued tasks is input, Parsed markdowns is output
        super().__init__(base_output_folder="parsed_content_markdowns", input_folder="queued_tasks")
        
        # Helper uses base_output_folder, but this script used output_folder
        # Alias it for minimal code change or update usages. Updating usages is better.
        # self.output_folder = self.base_output_folder 
        
        # self.summary_csv_path is already set by Helper to base_output_folder/_error_summary.csv
        # self.summary_fields is set by Helper

    def process_queued_tasks(self):
        # Get all JSON files from the queued_tasks folder (self.input_folder)
        if not os.path.exists(self.input_folder):
             print(f"❌ Input folder {self.input_folder} not found")
             return

        task_files = [f for f in os.listdir(self.input_folder) if f.endswith(".json")]

        if not task_files:
            print(f"No JSON files found in {self.input_folder}")
            return

        for file_name in task_files:
            file_path = os.path.join(self.input_folder, file_name)
            print(f"📂 Processing task file: {file_name}")

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"❌ Error reading {file_name}: {e}")
                continue

            # DataForSEO returns tasks in a 'tasks' list within the response
            tasks = data.get("tasks", [])
            payload = []

            for task in tasks:
                # print(task)
                task_id = task.get("id")
                # The original URL is usually in the 'data' block of the post response
                task_url = task.get("data", {}).get("start_url")

                if task_id and task_url:
                    payload.append({"id": task_id, "url": task_url})

            if payload:
                self.fetch_and_save_results(payload, file_name)
            else:
                print(f"⚠️ No valid IDs found in {file_name}")

    def fetch_and_save_results(self, payload, original_filename):
        endpoint = "https://api.dataforseo.com/v3/on_page/content_parsing"

        try:
            print(f"📡 Requesting content for {len(payload)} URLs...")
            response = requests.post(
                endpoint, headers=self.headers, json=payload, timeout=120
            )

            if response.status_code == 200:
                res = response.json()
                for i in res["tasks"]:
                    file_path = i.get("data", {}).get("tag", None)

                    if not file_path:
                        print("⚠️ No tag found in result")
                        continue

                    # Create a sub-directory named after the original file (without .json)
                    file_path = os.path.join(self.base_output_folder, file_path)
                    dir_name = os.path.dirname(file_path)

                    if not os.path.exists(dir_name):
                        try:
                            os.makedirs(dir_name)
                        except OSError:
                            pass

                    # Validate Result
                    status_msg = i.get("status_message")
                    # result field is a list, usually one item
                    result_list = i.get("result")
                    
                    is_valid = True
                    error_details = "Unknown Error"

                    if i.get("status_code") != 20000:
                        is_valid = False
                        error_details = f"API Error: {status_msg}"
                    elif not result_list:
                        is_valid = False
                        error_details = "Empty Result"
                    else:
                        # Check crawl status inside result
                        crawl_progress = result_list[0].get("crawl_progress")
                        crawl_status = result_list[0].get("crawl_status", {})
                        
                        if crawl_progress != "finished":
                            is_valid = False
                            error_details = f"Pending/Progress: {crawl_progress}"
                        elif crawl_status.get("pages_crawled", 0) == 0:
                             is_valid = False
                             error_details = "Crawl Failed (0 pages)"

                    if not is_valid:
                        # Log to CSV
                        print(f"⚠️ Invalid Result for {file_path}: {error_details}")
                        
                        # Extract metadata from file path/tag
                        try:
                            basename = os.path.basename(file_path)
                            # Parse parts
                            # type-local_pack_rg1_ra1_roofingabbotsfordbc.md
                            parts = basename.replace(".md", "").split("_")
                            item_type = parts[0].replace("type-", "")
                            rank_gp = parts[1].replace("rg", "")
                            rank = parts[2].replace("ra", "")
                            service = parts[3]                            
                            path_parts = file_path.split("/")
                            # Adjust path parts logic if needed, typically: output_folder/suburb/type/filename
                            # path_parts[-1] is filename, path_parts[-2] is type, path_parts[-3] is suburb?
                            # Check logic in on_page_post: self.base_output_folder, slugify(suburb), item_type
                            # So path is: base/suburb/type/file
                            
                            # Finding suburb from relative path in tag
                            # Tag usually: suburb/type/filename
                            tag_val = i.get("data", {}).get("tag", "")
                            tag_parts = tag_val.split("/")
                            suburb = tag_parts[0] if len(tag_parts) > 1 else "Unknown"
                            
                            url = i.get("data", {}).get("start_url", "Unknown")
                            
                            # @DEV fix it to write CRITICAL / ERROR IF organic and rg >1 < 5
                            status_issue = "PENDING" if "Pending" in error_details else "ERROR"

                            # Construct Row Data for Helper
                            row_data = {
                                CsvColumn.TYPE.value: item_type,
                                CsvColumn.RANK_ABSOLUTE.value: rank,
                                CsvColumn.RANK_GROUP.value: rank_gp,
                                CsvColumn.SUBURB.value: suburb,
                                CsvColumn.URL.value: url,
                                CsvColumn.SERVICE.value: service
                            }
                            
                            self.log_error_to_files(
                                row_data,
                                error_msg=error_details,
                                log_to_txt=False,
                                log_to_csv=True,
                                issue_override=status_issue
                            )

                        except Exception as e:
                            print(f"❌ Failed to log to CSV: {e}")

                    # Save the result anyway for debugging
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(i, f, indent=4)
                #
                # print(f"✅ Results saved to {file_path}/{file_name}")

            else:
                print(f"❌ API Error: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"❌ Connection Error: {str(e)}")


if __name__ == "__main__":
    fetcher = ResultFetcher()
    fetcher.process_queued_tasks()


# {'version': '0.1.20251226', 'status_code': 20000, 'status_message': 'Ok.', 'time': '0.1586 sec.', 'cost': 0, 'tasks_count': 8, 'tasks_error': 0,
# 'tasks': [{'id': '01140958-1303-0216-0000-b86bcc46294a',
# 'status_code': 20000, 'status_message': 'Ok.', 'time': '0.0172 sec.', 'cost': 0, 'result_count': 1, 'path': ['v3', 'on_page', 'content_p
# arsing'], 'data': {'api': 'on_page', 'function': 'content_parsing', 'url': 'https://www.roofingabbotsfordbc.com/', 'target': 'www.roofingabbotsfordbc.com', 'start_url':
# 'https://www.roofingabbotsfordbc.com/', 'enable_content_parsing': True, 'max_crawl_pages': 1, 'tag': 'Abbotsford-(NSW)/local_pack/type-local_pack_rg1_ra1_roofingabbotsfo
# rdbc.md'}, 'result': [{'crawl_progress': 'finished', 'crawl_status': {'max_crawl_pages': 1, 'pages_in_queue': 0, 'pages_crawled': 1}, 'items_count': 1, 'items': [{'type'
# : 'content_parsing_element', 'fetch_time': '2026-01-14 05:58:35 +00:00', 'status_code': 200, 'page_content': {'header':
