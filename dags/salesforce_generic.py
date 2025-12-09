from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
import json

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

def test_and_query_salesforce(**context):
    conn = BaseHook.get_connection('salesforce_generic')

    # Login host: login.salesforce.com or test.salesforce.com
    login_base = conn.host.rstrip('/')

    extra = json.loads(conn.extra) if conn.extra else {}
    client_id = extra["client_id"]
    client_secret = extra["client_secret"]
    refresh_token = extra["refresh_token"]

    # 1) Get fresh access token via refresh_token flow
    auth_url = f"{login_base}/services/oauth2/token"
    auth_data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }

    print(f"Auth URL: {auth_url}")
    auth_response = requests.post(auth_url, data=auth_data, timeout=30)

    if auth_response.status_code != 200:
        print("❌ Authentication (refresh token) failed")
        print(f"Status: {auth_response.status_code}")
        print(f"Response: {auth_response.text}")
        auth_response.raise_for_status()

    auth_result = auth_response.json()
    access_token = auth_result["access_token"]
    instance_url = auth_result["instance_url"]

    print("✅ Got access token via refresh_token flow")
    print(f"Instance URL: {instance_url}")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # 2) Test connection
    api_url = f"{instance_url}/services/data/"
    print(f"\n--- Testing Connection ---")
    print(f"Making request to: {api_url}")
    response = requests.get(api_url, headers=headers, timeout=30)
    response.raise_for_status()

    versions = response.json()
    latest_version = versions[-1]["version"]
    print(f"✅ Connection successful. Latest API version: {latest_version}")

    # 3) Query example
    print(f"\n--- Querying Salesforce ---")
    query = "SELECT Id, Name, Industry FROM Account LIMIT 5"
    query_url = f"{instance_url}/services/data/v{latest_version}/query"
    response = requests.get(query_url, headers=headers, params={"q": query}, timeout=30)
    response.raise_for_status()

    results = response.json()
    print(f"✅ Query successful! Total records: {results.get('totalSize', 0)}")

    for record in results.get("records", []):
        print(f"  - Account: {record.get('Name')} | Industry: {record.get('Industry', 'N/A')}")

    context["ti"].xcom_push(key="salesforce_test_status", value="success")
    context["ti"].xcom_push(key="records_retrieved", value=results.get("totalSize", 0))

    return f"✅ All tests passed! Retrieved {results.get('totalSize', 0)} accounts"


with DAG(
    dag_id='test_salesforce_generic',
    default_args=default_args,
    description='Test Salesforce connection using generic connector',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['salesforce', 'test', 'connection'],
) as dag:
    
    # Single task: Test connection and query (avoids token expiration between tasks)
    test_and_query = PythonOperator(
        task_id='test_and_query_salesforce',
        python_callable=test_and_query_salesforce,
    )