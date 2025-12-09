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

def test_salesforce_connection(**context):
    """
    Test Salesforce connection using the generic connector.
    Handles authentication and makes a simple API call.
    """
    try:
        # Get the connection details
        conn = BaseHook.get_connection('salesforce_generic_connection')
        
        # Extract connection details
        instance_url = conn.host  # e.g., https://your-instance.salesforce.com
        access_token = conn.password
        
        # Parse extra fields (JSON format)
        extra = json.loads(conn.extra) if conn.extra else {}
        
        # Log connection info (without sensitive data)
        print(f"Instance URL: {instance_url}")
        print(f"Connection ID: {conn.conn_id}")
        print(f"Extra fields: {list(extra.keys())}")
        
        # Test API call - Query Salesforce API version
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Simple test: Get API versions
        api_url = f"{instance_url}/services/data/"
        
        print(f"Making request to: {api_url}")
        response = requests.get(api_url, headers=headers, timeout=30)
        
        # Check response
        response.raise_for_status()
        
        versions = response.json()
        print(f"Connection successful!")
        print(f"Available API versions: {len(versions)}")
        print(f"Latest version: {versions[-1]['version']}")
        
        # Push results to XCom for downstream tasks
        context['ti'].xcom_push(key='salesforce_status', value='success')
        context['ti'].xcom_push(key='latest_api_version', value=versions[-1]['version'])
        
        return "Connection test passed!"
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response status: {e.response.status_code}")
        print(f"Response body: {e.response.text}")
        raise
        
    except requests.exceptions.Timeout:
        print(f"Timeout: Request to Salesforce timed out")
        raise
        
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error: Could not connect to Salesforce")
        print(f"Error details: {str(e)}")
        raise
        
    except json.JSONDecodeError as e:
        print(f"JSON Error: Could not parse connection extra field")
        print(f"Error: {str(e)}")
        raise
        
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        raise

def query_salesforce_accounts(**context):
    """
    Query Salesforce for Account records.
    This runs only if the connection test passes.
    """
    try:
        conn = BaseHook.get_connection('salesforce_generic')
        instance_url = conn.host
        access_token = conn.password
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Get latest API version from previous task
        api_version = context['ti'].xcom_pull(
            task_ids='test_connection', 
            key='latest_api_version'
        )
        
        # SOQL query for Accounts (limit to 5 for testing)
        query = "SELECT Id, Name, Industry FROM Account LIMIT 5"
        query_url = f"{instance_url}/services/data/v{api_version}/query"
        
        print(f"Querying: {query}")
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
        
        # Print account names
        for record in results.get('records', []):
            print(f"Account: {record.get('Name')} - Industry: {record.get('Industry', 'N/A')}")
        
        return f"Retrieved {results.get('totalSize', 0)} accounts"
        
    except Exception as e:
        print(f"Query failed: {str(e)}")
        raise

with DAG(
    dag_id='test_salesforce_generic_connection',
    default_args=default_args,
    description='Test Salesforce connection using generic connector',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['salesforce', 'test', 'connection'],
) as dag:
    
    # Task 1: Test basic connection
    test_conn = PythonOperator(
        task_id='test_connection',
        python_callable=test_salesforce_connection,
    )
    
    # Task 2: Query Salesforce (runs only if test passes)
    query_accounts = PythonOperator(
        task_id='query_accounts',
        python_callable=query_salesforce_accounts,
    )
    
    test_conn >> query_accounts