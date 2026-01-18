import csv
import os
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from dataforseo_client import configuration as dfs_config
from dataforseo_client import api_client as dfs_api_provider
from dataforseo_client.api.serp_api import SerpApi
from dataforseo_client.rest import ApiException
from config import USERNAME, PASSWORD


# ---------------- CONFIG ----------------
configuration = dfs_config.Configuration(
    username=USERNAME,
    password=PASSWORD
)

OUTPUT_DIR = "serp_outputs"
FIELDNAMES = [
    'rank_group', 'rank_absolute',
    'service', 'suburb',
    'title', 'domain', 'url',
    'description', 'type'
]

MAX_WORKERS = 5  # 🔥 tune this (depends on your API limits)


# ---------------- SYNC WORKER ----------------
def fetch_and_save_serp(service, suburb):
    """Runs inside thread pool"""
    try:
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        sub_clean = suburb.replace(' ', '-')
        ser_clean = service.replace(' ', '-')

        file_name = f"serp_{ser_clean}_{sub_clean}_{now}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)

        print(f"🚀 Searching: {service} in {suburb}")

        with dfs_api_provider.ApiClient(configuration) as api_client:
            serp_api = SerpApi(api_client)

            post_data = [{
                "keyword": f"{service} in {suburb}",
                "location_name": "Australia",
                "language_name": "English",
                "device": "mobile",
                "os": "ios",
                "depth": 20
            }]

            response = serp_api.google_organic_live_advanced(post_data)

            if not response.tasks:
                print(f"❌ No task result for {suburb}")
                return

            task = response.tasks[0]
            if task.status_message != "Ok." or not task.result:
                print(f"❌ No results for {suburb}")
                return

            items = task.result[0].items or []

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()

                for item in items:
                    writer.writerow({
                        'rank_group': getattr(item, 'rank_group', ''),
                        'rank_absolute': getattr(item, 'rank_absolute', ''),
                        'service': service,
                        'suburb': suburb,
                        'title': getattr(item, 'title', ''),
                        'domain': getattr(item, 'domain', ''),
                        'url': getattr(item, 'url', ''),
                        'description': getattr(item, 'description', ''),
                        'type': getattr(item, 'type', '')
                    })

        print(f"✅ Saved: {file_name}")

    except ApiException as e:
        print(f"🚫 API Error ({suburb}): {e}")
    except Exception as e:
        print(f"🔥 Error ({suburb}): {e}")


# ---------------- ASYNC ORCHESTRATOR ----------------
async def get_google_results_and_save_async(list_csv="list.csv"):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    tasks = []

    with open(list_csv, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        for row in reader:
            suburb = (row.get('Suburb') or row.get('suburb') or "").strip()
            service = (row.get('service') or row.get('Service') or "").strip()

            if not suburb or not service:
                continue

            task = loop.run_in_executor(
                executor,
                fetch_and_save_serp,
                service,
                suburb
            )
            tasks.append(task)

    await asyncio.gather(*tasks)
    executor.shutdown(wait=True)


# ---------------- ENTRY POINT ----------------
if __name__ == "__main__":
    asyncio.run(get_google_results_and_save_async())
