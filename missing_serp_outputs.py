import os
import csv
import json
import re
from base import Helper, CsvColumn

# ---- Missing File Checker ----
class MissingFileChecker(Helper):
    def __init__(self):
        super().__init__(base_output_folder="parsed_content_markdowns", input_folder="serp_outputs")
        # self.input_folder set by super

    def check_files(self):
        if not os.path.exists(self.input_folder):
             print(f"❌ Input folder {self.input_folder} not found")
             return

        csv_files = sorted(
            [f for f in os.listdir(self.input_folder) if f.endswith(".csv")]
        )

        print(f"🔍 Found {len(csv_files)} SERP CSV files.\n")

        for csv_filename in csv_files:
            full_path = os.path.join(self.input_folder, csv_filename)
            print(f"📄 Checking: {full_path}")

            with open(full_path, mode="r", encoding="utf-8") as file:
                reader = list(csv.DictReader(file))
                for row in reader:
                    # Use normalize_row from base
                    row_data = self.normalize_row(row)
                    
                    item_type = row_data[CsvColumn.TYPE.value]
                    url = row_data[CsvColumn.URL.value]
                    suburb = row_data[CsvColumn.SUBURB.value]
                    service = row_data[CsvColumn.SERVICE.value]
                    rank_abs = row_data[CsvColumn.RANK_ABSOLUTE.value]
                    rank_gp = row_data[CsvColumn.RANK_GROUP.value]

                    domain_match = self._extract_domain(url)
                    file_name = (
                        f"type-{item_type}_rg{rank_gp}_ra{rank_abs}_{domain_match}.md"
                    )
                    full_file_path = os.path.join(
                        self.base_output_folder, # Use base_output_folder
                        self._slugify(suburb),
                        self._slugify(item_type),
                        file_name,
                    )
                    if not os.path.exists(full_file_path):
                        print(f"❌ File Not Found: {full_file_path}")

                        self.log_error_to_files(
                            row_data,
                            error_msg="File Not Found"
                        )
                        full_dir_path = os.path.join(
                            self.base_output_folder, # Use base_output_folder
                            self._slugify(suburb),
                            self._slugify(item_type),
                        )
                        print(full_file_path)
                        if not os.path.exists(full_dir_path):
                            os.makedirs(full_dir_path)

                        with open(full_file_path, "w", encoding="utf-8") as f:
                            pass



if __name__ == "__main__":
    MissingFileChecker().check_files()
