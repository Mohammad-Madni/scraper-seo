import csv
import json
import os
from base import Helper, CsvColumn
# @DEV make error_summary creation here
PROGRESS_FILE = "parsing_progress.json"

class OnPageFetcher(Helper):
    def __init__(self):
        super().__init__(base_output_folder="queued_tasks", input_folder="serp_outputs")
        self.all_url_post_list = []
        
        # Validate input folder exists
        if not os.path.exists(self.input_folder):
            print(f"❌ Error: Input folder '{self.input_folder}' not found!")
            return
            
        # Process CSV files
        self.csv_files = sorted(
            [f for f in os.listdir(self.input_folder) if f.endswith(".csv")]
        )
        print(
            "🚀 Starting Scraping... (JS, Browser Rendering, and Switch Pool: ENABLED)"
        )

    def load_progress(self):
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        return {"last_file": None, "last_row_index": -1}

    def save_progress(self, filename, row_index):
        with open(PROGRESS_FILE, "w") as f:
            json.dump({"last_file": filename, "last_row_index": row_index}, f)

    def fetch_content_parsing_from_folder(
        self,
    ):
        """Fetch and parse content from CSV files in input folder"""
        # Load previous progress
        progress = self.load_progress()
        last_file = progress["last_file"]
        last_row_idx = progress["last_row_index"]
        self._initialize_summary_csv()


        for csv_filename in self.csv_files:
            # Skip already processed files
            if last_file and csv_filename < last_file:
                continue

            csv_path = os.path.join(self.input_folder, csv_filename)

            try:
                with open(csv_path, mode="r", encoding="utf-8") as file:
                    reader = list(csv.DictReader(file))

                    for idx, row in enumerate(reader):
                        # Skip already processed rows
                        if csv_filename == last_file and idx <= last_row_idx:
                            continue

                        # Extract row data using base helper
                        row_data = self.normalize_row(row)
                        
                        item_type = row_data[CsvColumn.TYPE.value]
                        url = row_data[CsvColumn.URL.value]
                        suburb = row_data[CsvColumn.SUBURB.value]
                        # service = row_data[CsvColumn.SERVICE.value] # unused variable in logic below
                        rank_abs = row_data[CsvColumn.RANK_ABSOLUTE.value]
                        rank_gp = row_data[CsvColumn.RANK_GROUP.value]
                        domain = row_data[CsvColumn.DOMAIN.value]

                        # Original logic used _slugify from Helper (now in base)
                        type_path = os.path.join(
                            self.base_output_folder, self._slugify(suburb), item_type
                        )

                        domain_match = self._extract_domain(url)
                        file_name = f"type-{item_type}_rg{rank_gp}_ra{rank_abs}_{domain_match}.md"
                        file_path = os.path.join(
                            self._slugify(suburb), self._slugify(item_type), file_name
                        )

                        # the base dir is not passed in tag @DEV
                        if url and url.startswith("http") and "google.com" not in url:
                            print(f"🔍 Parsing {domain_match} (Rank {rank_abs})...")
                            self.all_url_post_list.append(
                                {
                                    "target": domain,
                                    "start_url": url,
                                    "enable_content_parsing": True,
                                    "max_crawl_pages": 1,
                                    "tag": file_path,
                                }
                            )

                        else:


                            print(f"📄 Saving Meta for Rank {rank_abs}")
                            file_path = os.path.join(
                                    "parsed_content_markdowns", file_path
                            )

                            dir_name = file_path.split("/")[:-1]
                            dir_name = os.path.join(*dir_name)

                            if not os.path.exists(dir_name):
                                os.makedirs(dir_name)


                            with open(file_path, "w", encoding="utf-8") as f:
                                f.write(
                                    f"# Type: {item_type} | Rank: {rank_abs} | RG: {rank_gp}\n"
                                )
                                f.write("### Raw Row Data:\n")
                                json.dump(row, f, indent=4)

            except KeyboardInterrupt:
                print("\n🛑 Stopped by user. Progress saved.")
                return
            except Exception as e:
                print(f"❌ Error processing file {csv_filename}: {e}")

            print("📡 Sending batch request...")
            for i in self.all_url_post_list:
                print(i["tag"])

            # Post any remaining tasks for the file
            if self.all_url_post_list:
                self.post_tasks(csv_filename, idx)

        if self.all_url_post_list:
            self.post_tasks(csv_filename, idx, end=True)

    def post_tasks(self,csv_filename, idx, end:bool= False):
        if not csv_filename:
            csv_filename = "unknown"

        if end or len(self.all_url_post_list) > 50:
             self.post_onpage_task(csv_filename, idx)

    def post_onpage_task(
        self,
        csv_filename,
        idx,
    ):
        from post_page import post_onpage_task as shared_post_task
        
        # Use shared helper
        import time
        # only use date here @DEV
        safe_name = f"{csv_filename.replace('.csv', '')}_{int(time.time())}.json"
        
        resp = shared_post_task(self.headers, self.all_url_post_list, output_dir=self.base_output_folder, filename=safe_name)
        
        if resp and resp.status_code == 200:
             print(f"✅ Posted batch of {len(self.all_url_post_list)} tasks.")
        else:
             print(f"⚠️ Batch post failed.")

        self.all_url_post_list = []
        self.save_progress(csv_filename, idx)


OnPageFetcher().fetch_content_parsing_from_folder()
