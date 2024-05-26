import json

import requests
import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def get_data(**kwargs):
    url = "https://raw.githubusercontent.com/svc98/AWS_Sales_Events_Batch/main/data/sales_data_sample.csv"
    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(url, header=0)

        # Convert DF to JSON String for XCOM
        json_data = df.to_json(orient='records', lines=True)
        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception(f'Failed to get data, HTTP status code: {response.status_code}')


def preview_data(**kwargs):
    output_data = kwargs['ti'].xcom_pull(keys='data', task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from XCOM')

    # Create DF from OutputData
    df = pd.DataFrame(output_data)
    df = df.groupby('PRODUCTLINE', as_index=False).agg({'QUANTITYORDERED': 'sum', 'SALES': 'sum'})

    # Sorting
    df = df.sort_values(by='SALES', ascending=False)

    print(df[['PRODUCTLINE', 'QUANTITYORDERED', 'SALES']].head(10))


default_args = {
    'owner': 'svc',
    'start_date': datetime(2024, 5, 26),
    'catchup': False
}

dag = DAG(
    'sales_data_preview',
    default_args=default_args,
    schedule=timedelta(days=1)
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