import os
import csv
import json
import requests
import base64
from config import USERNAME, PASSWORD
from base import Helper, CsvColumn
import time
import asyncio
import aiohttp
import aiofiles
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple


class ResultFetcher(Helper):
    def __init__(self, max_concurrent_requests=10, max_workers=4):
        # Queued tasks is input, Parsed markdowns is output
        super().__init__(base_output_folder="parsed_content_markdowns", input_folder="queued_tasks")
        
        # Configuration for concurrency
        self.max_concurrent_requests = max_concurrent_requests  # Max simultaneous API calls
        self.max_workers = max_workers  # Thread pool size for file I/O
        self.semaphore = None  # Will be initialized in async context

    def process_queued_tasks(self):
        """Main entry point - runs async processing"""
        asyncio.run(self._async_process_queued_tasks())

    async def _async_process_queued_tasks(self):
        """Async version of process_queued_tasks"""
        # Initialize semaphore in async context
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        # Get all JSON files from the queued_tasks folder
        if not os.path.exists(self.input_folder):
            print(f"❌ Input folder {self.input_folder} not found")
            return

        task_files = [f for f in os.listdir(self.input_folder) if f.endswith(".json")]

        if not task_files:
            print(f"No JSON files found in {self.input_folder}")
            return

        # Process all files concurrently
        tasks = []
        for file_name in task_files:
            tasks.append(self._process_single_file(file_name))
        
        # Wait for all files to be processed
        await asyncio.gather(*tasks)

    async def _process_single_file(self, file_name: str):
        """Process a single task file asynchronously"""
        file_path = os.path.join(self.input_folder, file_name)
        print(f"📂 Processing task file: {file_name}")

        try:
            # Read file asynchronously
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                content = await f.read()
                data = json.loads(content)
        except Exception as e:
            print(f"❌ Error reading {file_name}: {e}")
            return

        # DataForSEO returns tasks in a 'tasks' list within the response
        tasks = data.get("tasks", [])
        payload = []

        for task in tasks:
            task_id = task.get("id")
            task_url = task.get("data", {}).get("start_url")

            if task_id and task_url:
                payload.append({"id": task_id, "url": task_url})

        if payload:
            await self.fetch_and_save_results(payload, file_name)
        else:
            print(f"⚠️ No valid IDs found in {file_name}")

    async def fetch_and_save_results(self, payload: List[Dict], original_filename: str):
        """Async version of fetch_and_save_results"""
        endpoint = "https://api.dataforseo.com/v3/on_page/content_parsing"

        try:
            print(f"📡 Requesting content for {len(payload)} URLs...")
            
            # Use semaphore to limit concurrent requests
            async with self.semaphore:
                async with aiohttp.ClientSession() as session:
                    # Prepare auth
                    auth = aiohttp.BasicAuth(USERNAME, PASSWORD)
                    
                    async with session.post(
                        endpoint,
                        json=payload,
                        auth=auth,
                        timeout=aiohttp.ClientTimeout(total=120),
                        headers={"Content-Type": "application/json"}
                    ) as response:
                        if response.status == 200:
                            res = await response.json()
                            
                            # Process results concurrently using thread pool for I/O
                            save_tasks = []
                            for i in res["tasks"]:
                                save_tasks.append(self._process_and_save_result(i))
                            
                            # Wait for all saves to complete
                            await asyncio.gather(*save_tasks)
                        else:
                            text = await response.text()
                            print(f"❌ API Error: {response.status} - {text}")

        except asyncio.TimeoutError:
            print(f"❌ Timeout Error for {original_filename}")
        except Exception as e:
            print(f"❌ Connection Error: {str(e)}")

    async def _process_and_save_result(self, task_result: Dict):
        """Process and save a single result asynchronously"""
        file_path = task_result.get("data", {}).get("tag", None)

        if not file_path:
            print("⚠️ No tag found in result")
            return

        # Create a sub-directory named after the original file
        file_path = os.path.join(self.base_output_folder, file_path)
        dir_name = os.path.dirname(file_path)

        # Create directory if needed (use thread pool for I/O)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._ensure_directory, dir_name)

        # Validate Result
        status_msg = task_result.get("status_message")
        result_list = task_result.get("result")
        
        is_valid = True
        error_details = "Unknown Error"

        if task_result.get("status_code") != 20000:
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
            # Log to CSV (run in thread pool to avoid blocking)
            print(f"⚠️ Invalid Result for {file_path}: {error_details}")
            await loop.run_in_executor(None, self._log_error, task_result, file_path, error_details)

        # Save the result asynchronously
        try:
            async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
                await f.write(json.dumps(task_result, indent=4))
        except Exception as e:
            print(f"❌ Failed to save {file_path}: {e}")

    def _ensure_directory(self, dir_name: str):
        """Ensure directory exists (thread-safe)"""
        if not os.path.exists(dir_name):
            try:
                os.makedirs(dir_name, exist_ok=True)
            except OSError:
                pass

    def _log_error(self, task_result: Dict, file_path: str, error_details: str):
        """Log error to CSV (thread-safe helper)"""
        try:
            basename = os.path.basename(file_path)
            # Parse parts
            parts = basename.replace(".md", "").split("_")
            item_type = parts[0].replace("type-", "")
            rank_gp = parts[1].replace("rg", "")
            rank = parts[2].replace("ra", "")
            service = parts[3]
            
            # Finding suburb from tag
            tag_val = task_result.get("data", {}).get("tag", "")
            tag_parts = tag_val.split("/")
            suburb = tag_parts[0] if len(tag_parts) > 1 else "Unknown"
            
            url = task_result.get("data", {}).get("start_url", "Unknown")
            
            status_issue = "PENDING" if "Pending" in error_details else "ERROR"

            # Construct Row Data
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


if __name__ == "__main__":
    # You can adjust these parameters for optimal performance
    # max_concurrent_requests: How many API calls to make simultaneously (default: 10)
    # max_workers: Thread pool size for file I/O operations (default: 4)
    fetcher = ResultFetcher(max_concurrent_requests=10, max_workers=5)
    
    start_time = time.time()
    fetcher.process_queued_tasks()
    elapsed = time.time() - start_time
    
    print(f"\n✅ All tasks completed in {elapsed:.2f} seconds")

# {'version': '0.1.20251226', 'status_code': 20000, 'status_message': 'Ok.', 'time': '0.1586 sec.', 'cost': 0, 'tasks_count': 8, 'tasks_error': 0,
# 'tasks': [{'id': '01140958-1303-0216-0000-b86bcc46294a',
# 'status_code': 20000, 'status_message': 'Ok.', 'time': '0.0172 sec.', 'cost': 0, 'result_count': 1, 'path': ['v3', 'on_page', 'content_p
# arsing'], 'data': {'api': 'on_page', 'function': 'content_parsing', 'url': 'https://www.roofingabbotsfordbc.com/', 'target': 'www.roofingabbotsfordbc.com', 'start_url':
# 'https://www.roofingabbotsfordbc.com/', 'enable_content_parsing': True, 'max_crawl_pages': 1, 'tag': 'Abbotsford-(NSW)/local_pack/type-local_pack_rg1_ra1_roofingabbotsfo
# rdbc.md'}, 'result': [{'crawl_progress': 'finished', 'crawl_status': {'max_crawl_pages': 1, 'pages_in_queue': 0, 'pages_crawled': 1}, 'items_count': 1, 'items': [{'type'
# : 'content_parsing_element', 'fetch_time': '2026-01-14 05:58:35 +00:00', 'status_code': 200, 'page_content': {'header':
