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
    """
    Test Salesforce connection and query in one task to avoid token expiration.
    """
    try:
        # Get the connection details
        conn = BaseHook.get_connection('salesforce_generic')
        
        # Extract connection details
        instance_url = conn.host
        access_token = conn.password
        
        # Parse extra fields
        extra = json.loads(conn.extra) if conn.extra else {}
        
        print(f"Instance URL: {instance_url}")
        print(f"Connection ID: {conn.conn_id}")
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Test 1: Get API versions
        api_url = f"{instance_url}/services/data/"
        print(f"\n--- Testing Connection ---")
        print(f"Making request to: {api_url}")
        
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        versions = response.json()
        print(f"Connection successful!")
        print(f"Available API versions: {len(versions)}")
        latest_version = versions[-1]['version']
        print(f"Latest version: {latest_version}")
        
        # Test 2: Query Accounts (same token, same task)
        print(f"\n--- Querying Salesforce ---")
        query = "SELECT Id, Name, Industry FROM Account LIMIT 5"
        query_url = f"{instance_url}/services/data/v{latest_version}/query"
        
        print(f"Query: {query}")
        response = requests.get(
            query_url,
            headers=headers,
            params={'q': query},
            timeout=30
        )
        
        response.raise_for_status()
        results = response.json()
        
        print(f"Query successful!")
        print(f"Total records: {results.get('totalSize', 0)}")
        
        # Print account details
        for record in results.get('records', []):
            print(f"  - Account: {record.get('Name')} | Industry: {record.get('Industry', 'N/A')}")
        
        # Push results to XCom
        context['ti'].xcom_push(key='salesforce_test_status', value='success')
        context['ti'].xcom_push(key='records_retrieved', value=results.get('totalSize', 0))
        
        return f"All tests passed! Retrieved {results.get('totalSize', 0)} accounts"
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Status Code: {e.response.status_code}")
        print(f"Response: {e.response.text}")
        
        # Provide helpful hints based on status code
        if e.response.status_code == 401:
            print("\nToken expired or invalid. Try:")
            print("   1. Generate a new access token")
            print("   2. Use Connected App with refresh token")
            print("   3. Check if IP restrictions apply")
        elif e.response.status_code == 403:
            print("\nPermission denied. Check:")
            print("   1. User has API access enabled")
            print("   2. User has permission to query Accounts")
        
        raise
        
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        raise

with DAG(
    dag_id='test_salesforce_generic_connection',
    default_args=default_args,
    description='Test Salesforce connection using generic connector',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generic', 'salesforce', 'test', 'connection'],
) as dag:
    
    # Single task: Test connection and query (avoids token expiration between tasks)
    test_and_query = PythonOperator(
        task_id='test_and_query_salesforce',
        python_callable=test_and_query_salesforce,
    )