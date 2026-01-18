import csv
import os
from datetime import datetime
from dataforseo_client import configuration as dfs_config
from dataforseo_client import api_client as dfs_api_provider
from dataforseo_client.api.serp_api import SerpApi
from dataforseo_client.rest import ApiException
from config import USERNAME,PASSWORD

configuration = dfs_config.Configuration(
    username=USERNAME,
    password=PASSWORD
)

def get_google_results_and_save(list_csv="list.csv"):
    output_dir = "serp_outputs"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Folder '{output_dir}' created.")

    fieldnames = ['rank_group', 'rank_absolute', 'service', 'suburb', 'title', 'domain', 'url', 'description', 'type']

    try:
        with open(list_csv, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            for row in reader:
                current_suburb = (row.get('Suburb') or row.get('suburb') or "").strip()
                current_service = (row.get('service') or row.get('Service') or "").strip()
                
                if not current_suburb or not current_service:
                    continue

                now = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                sub_clean = current_suburb.replace(' ', '-')
                ser_clean = current_service.replace(' ', '-')
                
                file_name = f"serp_{ser_clean}_{sub_clean}_{now}.csv"
                file_path = os.path.join(output_dir, file_name)

                print(f"🚀 Searching for: {current_service} in {current_suburb}...")

                with dfs_api_provider.ApiClient(configuration) as api_client:
                    serp_api = SerpApi(api_client)
                    post_data = [{
                        "keyword": f"{current_service} in {current_suburb}",
                        "location_name": "Australia",
                        "language_name": "English",
                        "device": "mobile",
                        "os": "ios",
                        "depth": 20
                    }]

                    try:
                        response = serp_api.google_organic_live_advanced(post_data)
                        if response.tasks and len(response.tasks) > 0:
                            task = response.tasks[0]
                            if task.status_message == "Ok." and task.result:
                                items = task.result[0].items
                                
                                with open(file_path, mode='w', newline='', encoding='utf-8') as outfile:
                                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                                    writer.writeheader()
                                    
                                    for item in items:
                                        writer.writerow({
                                            'rank_group': getattr(item, 'rank_group', ''),
                                            'rank_absolute': getattr(item, 'rank_absolute', ''),
                                            'service': current_service,
                                            'suburb': current_suburb,
                                            'title': getattr(item, 'title', ''),
                                            'domain': getattr(item, 'domain', ''),
                                            'url': getattr(item, 'url', ''),
                                            'description': getattr(item, 'description', ''),
                                            'type': getattr(item, 'type', '')
                                        })
                                print(f"✅ Results saved to: {file_name}")
                            else:
                                print(f"❌ No results for {current_suburb}")
                    except ApiException as e:
                        print(f"🚫 API Error for {current_suburb}: {e}")

    except FileNotFoundError:
        print(f"❌ فایل ورودی {list_csv} پیدا نشد.")

if __name__ == "__main__":
    get_google_results_and_save()

