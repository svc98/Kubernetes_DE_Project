from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


def get_data(**kwargs):
    import requests
    import pandas as pd

    url = 'https://raw.githubusercontent.com/airscholar/ApacheFlink-SalesAnalytics/main/output/new-output.csv'
    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(url, header=None, names=['Category', 'Price', 'Quantity'])

        # Convert DF to JSON string from XCOM
        json_data = df.to_json(orient='records')

        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception(f'Failed to get data, HTTP status code: {response.status_code}')


def preview_data(**kwargs):
    import pandas as pd
    import json

    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from XCom')

    # Create DF from JSON data
    df = pd.DataFrame(output_data)

    # Compute total sales
    df['Total'] = df['Price'] * df['Quantity']

    df = df.groupby('Category', as_index=False).agg({'Quantity': 'sum', 'Total': 'sum'})

    # Sort by total sales
    df = df.sort_values(by='Total', ascending=False)

    print(df[['Category', 'Total']].head(20))


default_args = {
    'owner': 'svc',
    'start_date': datetime(2024, 5, 26),
    'catchup': False
}

dag = DAG(
    'sales_data_preview',
    default_args=default_args,
    schedule="@daily"
)

t1 = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

t2 = PythonOperator(
    task_id='preview_data',
    python_callable=preview_data,
    dag=dag
)

t1 >> t2
