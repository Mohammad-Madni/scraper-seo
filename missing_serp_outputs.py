import os
import csv
import json
import re

# ---- match your original classes ----
PROGRESS_FILE = "parsing_progress.json"


class Helper:
    def __init__(self):
        # base folder where parsed markdowns should be
        self.base_output_folder = "parsed_content_markdowns"
        self.summary_path = "parsed_content_markdowns/_error_summary.csv"
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

    def _slugify(self, text):
        return str(text).strip().replace(" ", "-")

    def _extract_domain(self, url):
        if not url:
            return "metadata"
        return re.sub(r"(https?://|www\.)", "", url).split("/")[0].split(".")[0]

    def log_error_to_files(
        self,
        type_path,
        error_msg,
        item_type,
        rank_abs,
        rank_gp,
        suburb,
        service,
        url,
    ):
        """ثبت خطاها در فایل‌های مربوطه"""
        try:
            rank_int = int(rank_abs) if str(rank_abs).isdigit() else 0
        except:
            rank_int = 0

        issue_label = "CRITICAL" if rank_int <= 5 else "Error"
        log_name = "warning.txt" if rank_int <= 5 else "error.txt"

        item_path = os.path.join(
            "parsed_content_markdowns", self._slugify(suburb), self._slugify(item_type)
        )
        if not os.path.exists(item_path):
            os.makedirs(item_path)

        with open(os.path.join(item_path, log_name), "a", encoding="utf-8") as f:
            f.write(
                f"[{issue_label}] Rank: {rank_abs} (RG: {rank_gp}) | Error: {error_msg} | URL: {url}\n"
             )

        with open(self.summary_path, "a", newline="", encoding="utf-8") as f:
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



# ---- Missing File Checker ----
class MissingFileChecker(Helper):
    def __init__(self):
        super().__init__()
        self.input_folder = "serp_outputs"

    def check_files(self):
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
                    item_type = str(row.get("type", "other")).lower().replace(" ", "_")
                    url = row.get("url") or ""
                    suburb = row.get("suburb") or row.get("Suburb") or "Unknown"
                    service = row.get("service") or row.get("Service") or "service"
                    rank_abs = row.get("rank_absolute", "0")
                    rank_gp = row.get("rank_group", "0")

                    domain_match = self._extract_domain(url)
                    file_name = (
                        f"type-{item_type}_rg{rank_gp}_ra{rank_abs}_{domain_match}.md"
                    )
                    full_file_path = os.path.join(
                        "parsed_content_markdowns",
                        self._slugify(suburb),
                        self._slugify(item_type),
                        file_name,
                    )
                    if not os.path.exists(full_file_path):
                        print(f"❌ File Not Found: {full_file_path}")

                        self.log_error_to_files(
                            type_path=os.path.join(item_type),
                            error_msg="File Not Found",
                            item_type=item_type,
                            rank_abs=rank_abs,
                            rank_gp=rank_gp,
                            suburb=suburb,
                            service=service,
                            url=url,
                        )


if __name__ == "__main__":
    MissingFileChecker().check_files()
