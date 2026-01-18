import os
import requests
import json
import time

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def post_onpage_task(headers, payload, output_dir="queued_tasks", filename=None, timeout=120):
    """Post a list of on_page tasks to DataForSEO `task_post` endpoint.

    Saves the raw response JSON to `output_dir/filename` if provided.
    Returns the requests.Response object.
    """
    ensure_dir(output_dir)

    endpoint = "https://api.dataforseo.com/v3/on_page/task_post"

    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
    except Exception as e:
        print(f"❌ Post Request Failed: {e}")
        return None

    # Prepare filename
    if filename:
        out_path = os.path.join(output_dir, filename)
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(resp.json(), f, indent=4)
        except Exception:
            # If response body isn't JSON, save raw text
            try:
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)
            except Exception:
                pass

    return resp

def poll_task_results(headers, task_ids, timeout=1200, check_interval=10):
    """
    Polls DataForSeo `task_get` endpoint for a list of task IDs.
    Returns a dictionary mapping task_id -> result_item (or None if failed).
    
    DataForSeo OnPage tasks usually take some time.
    """
    results_map = {}
    pending_ids = list(task_ids)
    
    start_time = time.time()
    
    while pending_ids:
        if time.time() - start_time > timeout:
            print("⏰ Polling timed out!")
            break
            
        # Check tasks one by one (or batch if API supports specific batch get, 
        # but standard way is individual or 'tasks_ready'). 
        # Optimizing: DataForSEO has 'summary' or 'id' endpoints. 
        # We will loop through pending IDs. To avoid hitting rate limits, we sleep.
        
        # Note: 'task_get' can return multiple tasks if we use the right endpoint or logic, 
        # but standard pattern for simple integration:
        
        remaining_ids = []
        
        for tid in pending_ids:
            # Endpoint for specific task
            endpoint = f"https://api.dataforseo.com/v3/on_page/task_get/regular/{tid}"
            try:
                r = requests.get(endpoint, headers=headers)
                if r.status_code == 200:
                    data = r.json()
                    # Check task status
                    # structure: data['tasks'][0]['result'][0]...
                    task_data = data.get('tasks', [{}])[0]
                    
                    # DataForSeo OnPage 'task_get' returns the result directly if ready?
                    # Actually for OnPage specifically, 'task_post' is for crawl, 'task_get' gets the result.
                    # If it's 200 OK, it usually means result is there.
                    # We should check if 'result' is not null.
                    
                    if task_data.get('status_message') == 'Ok.':
                        results_map[tid] = task_data
                    else:
                        # If invalid ID or error, stop checking this one
                         print(f"⚠️ Task {tid} Error: {task_data.get('status_message')}")
                         results_map[tid] = None
                else:
                    # If not ready, it might return 404 or specific code? 
                    # Actually DataForSeo returns 200 but maybe 'status_code' inside JSON differs?
                    # For OnPage, usually we wait. 
                    remaining_ids.append(tid)
                    
            except Exception as e:
                print(f"⚠️ Polling Error for {tid}: {e}")
                remaining_ids.append(tid)
                
            time.sleep(0.5) # small delay between requests

        pending_ids = remaining_ids
        if not pending_ids:
            break
            
        print(f"⏳ Waiting for {len(pending_ids)} tasks... (Elapsed: {int(time.time()-start_time)}s)")
        time.sleep(check_interval)

    return results_map
