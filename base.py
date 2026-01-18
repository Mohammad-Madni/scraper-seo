import os
import csv
import re
import json
import base64
from enum import Enum
from config import USERNAME, PASSWORD

class CsvColumn(Enum):
    TYPE = "type"
    URL = "url"
    SUBURB = "suburb"
    SERVICE = "service"
    RANK_ABSOLUTE = "rank_absolute"
    RANK_GROUP = "rank_group"
    DOMAIN = "domain"

class Helper:
    def __init__(self, base_output_folder="parsed_content_markdowns", input_folder="serp_outputs"):
        # Setup authentication and headers
        auth_str = f"{USERNAME}:{PASSWORD}"
        self.token = base64.b64encode(auth_str.encode()).decode()
        self.headers = self._create_headers()
        
        self.input_folder = input_folder
        self.base_output_folder = base_output_folder
        self.summary_csv_path = os.path.join(
            self.base_output_folder, "_error_summary.csv"
        )
        self.summary_fields = [
            "Issue",
            "suburb",
            "service",
            "type",
            "rank",
            "rank_group",
            "url",
            "error_type",
            "status",
        ]

        # Setup output directory
        if not os.path.exists(self.base_output_folder):
             try:
                os.makedirs(self.base_output_folder)
             except OSError:
                pass # Already exists

    def _create_headers(self):
        """Create API request headers"""
        return {
            "Authorization": f"Basic {self.token}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

    def _initialize_summary_csv(self):
        """Initialize error summary CSV file if it doesn't exist"""
        # Ensure directory exists first
        if not os.path.exists(os.path.dirname(self.summary_csv_path)):
             os.makedirs(os.path.dirname(self.summary_csv_path))
             
        if not os.path.exists(self.summary_csv_path):
            print("📡 Creating error summary CSV...")
            with open(self.summary_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.summary_fields)
                writer.writeheader()

    def _slugify(self, text):
        """Convert text to slug format (e.g., 'New York' -> 'New-York')"""
        return str(text).strip().replace(" ", "-")

    def _extract_domain(self, url):
        """Extract domain name from URL"""
        if not url:
            return "metadata"
        return re.sub(r"(https?://|www\.)", "", url).split("/")[0].split(".")[0]

    def normalize_row(self, row):
        """Standardize row keys and values"""
        return {
            CsvColumn.TYPE.value: str(row.get("type", "other")).lower().replace(" ", "_"),
            CsvColumn.URL.value: row.get("url") or "",
            CsvColumn.SUBURB.value: row.get("suburb") or row.get("Suburb") or "Unknown",
            CsvColumn.SERVICE.value: row.get("service") or row.get("Service") or "service",
            CsvColumn.RANK_ABSOLUTE.value: row.get("rank_absolute", "0"),
            CsvColumn.RANK_GROUP.value: row.get("rank_group", "0"),
            CsvColumn.DOMAIN.value: row.get("domain") or ""
        }

    def log_error_to_files(
        self,
        row_data, # Expects normalized row data or similar dict
        error_msg,
        log_to_txt=True,
        log_to_csv=True,
        issue_override=None
    ):
        
        # Unpack commonly used fields
        item_type = row_data.get(CsvColumn.TYPE.value, "other")
        rank_abs = row_data.get(CsvColumn.RANK_ABSOLUTE.value, "0")
        rank_gp = row_data.get(CsvColumn.RANK_GROUP.value, "0")
        suburb = row_data.get(CsvColumn.SUBURB.value, "Unknown")
        service = row_data.get(CsvColumn.SERVICE.value, "service")
        url = row_data.get(CsvColumn.URL.value, "")

        try:
            rank_int = int(rank_abs) if str(rank_abs).isdigit() else 0
        except:
            rank_int = 0

        # Determine Issue Label
        if issue_override:
            issue_label = issue_override
        else:
            issue_label = "CRITICAL" if rank_int <= 5 else "Error"
            
        log_name = "warning.txt" if rank_int <= 5 else "error.txt"

        # Log to TXT in type directory
        if log_to_txt:
            item_path = os.path.join(
                self.base_output_folder, self._slugify(suburb), self._slugify(item_type)
            )
            if not os.path.exists(item_path):
                os.makedirs(item_path)

            with open(os.path.join(item_path, log_name), "a", encoding="utf-8") as f:
                f.write(
                    f"[{issue_label}] Rank: {rank_abs} (RG: {rank_gp}) | Error: {error_msg} | URL: {url}\n"
                )

        # Log to CSV
        if log_to_csv:
            self._initialize_summary_csv()
            with open(self.summary_csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.summary_fields)
                writer.writerow(
                    {
                        "Issue": issue_label,
                        "suburb": suburb,
                        "service": service,
                        "type": item_type,
                        "rank": rank_abs,
                        "rank_group": rank_gp,
                        "url": url,
                        "error_type": log_name.replace(".txt", ""),
                        "status": error_msg,
                    }
                )
