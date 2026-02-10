from azure.identity import DefaultAzureCredential
from datetime import datetime, timedelta
import requests
import json

WORKSPACE = "test-synapse-jervis"
API_VERSION = "2020-12-01"
ENDPOINT = f"https://{WORKSPACE}.dev.azuresynapse.net"


def get_access_token():
    """Get Azure access token for Synapse data-plane."""
    cred = DefaultAzureCredential()
    return cred.get_token("https://dev.azuresynapse.net/.default").token


def get_pipeline_runs(
    days_back=7,
    pipeline_name=None,
    start_time=None,
    end_time=None,
):
    """
    Fetch Synapse pipeline runs by workspace.
    """
    token = get_access_token()

    if start_time and end_time:
        start = start_time
        end = end_time
    else:
        end = datetime.utcnow()
        start = end - timedelta(days=days_back)

    url = f"{ENDPOINT}/queryPipelineRuns?api-version={API_VERSION}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    body = {
        "lastUpdatedAfter": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "lastUpdatedBefore": end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }

    if pipeline_name:
        body["filters"] = [
            {
                "operand": "PipelineName",
                "operator": "Equals",
                "values": [pipeline_name],
            }
        ]

    resp = requests.post(url, headers=headers, json=body)
    resp.raise_for_status()

    print(f"response: {resp.json()}")

    return resp.json().get("value", [])

def get_activity_runs(pipeline_name, pipeline_run_id, run_start, run_end):
    """
    Fetch activity runs using Synapse data-plane queryActivityruns (note lowercase 'r').
    """
    token = get_access_token()

    # pipeline_name = "PipelineWaitTest"
    # pipeline_run_id = "6d87c205-28c4-4047-8e2a-64a3e4642e67"

    url = f"{ENDPOINT}/pipelines/{pipeline_name}/pipelineruns/{pipeline_run_id}/queryActivityruns?api-version={API_VERSION}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # run_start=datetime(2026, 2, 1)
    # run_end=datetime(2026, 2, 12)

    body = {
        "lastUpdatedAfter": run_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "lastUpdatedBefore": run_end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }

    resp = requests.post(url, headers=headers, json=body)
    print(f"Activity status: {resp.status_code}")
    if resp.status_code != 200:
        print(f"Activity response: {resp.text}")

    resp.raise_for_status()
    data = resp.json()
    print("Activity runs raw response:", data)
    
    # # Write to test2.json with proper formatting
    # with open('test2.json', 'w', encoding='utf-8') as f:
    #     json.dump(data, f, indent=2, ensure_ascii=False)
    
    # print("\nResponse written to test2.json")
    
    return

def get_notebook_output(run_id):
    """
    Fetch notebook output snapshot from Synapse pipeline notebook run.
    """
    token = get_access_token()

    url = f"{ENDPOINT}/runnotebookapi/versions/2022-03-01-preview/pipelinerun/{run_id}/snapshot"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    
    data = resp.json()
    
    # Write to file
    with open('notebook_output.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("Notebook output written to notebook_output.json")
    
    return data

if __name__ == "__main__":
    # runs = get_pipeline_runs(
    #     start_time=datetime(2026, 2, 1),
    #     end_time=datetime(2026, 2, 10),
    # )

    # print(f"\nFound {len(runs)} pipeline runs.\n")

    # for i, r in enumerate(runs, 1):
    #     print("=" * 80)
    #     print(f"Run {i}: {r['pipelineName']} - {r['status']}")
    #     print(f"Run ID: {r['runId']}")
    #     print(f"Start: {r['runStart']}, End: {r['runEnd']}")
    #     print(f"Duration: {r['durationInMs']}ms")
    #     if r.get("message"):
    #         print(f"Message: {r['message']}")

        # print("\nFetching activity runs...")
        # try:
        #     activity_runs = get_activity_runs(
        #         r["runId"], 
        #         r["runStart"], 
        #         r["runEnd"]
        #     )
        # except requests.HTTPError as e:
        #     print(f"  Error calling activity runs API: {e}")
        #     continue

        # if activity_runs:
        #     print(f"\nActivities ({len(activity_runs)}):")
        #     for act in activity_runs:
        #         print(f"\n  Activity: {act['activityName']} ({act['activityType']})")
        #         print(f"  Status: {act['status']}")
        #         print(f"  Start: {act.get('activityRunStart', 'N/A')}")
        #         print(f"  End: {act.get('activityRunEnd', 'N/A')}")
        #         print(f"  Duration: {act.get('durationInMs', 0)}ms")
        #         if act.get("error"):
        #             print(f"  Error: {act['error']}")
        # else:
        #     print("  No activity runs found.")

        # print()

    notebook_run_id = "ebad2039-554a-4f77-8bd0-58e03dbb4575"

    get_notebook_output(notebook_run_id)