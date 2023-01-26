from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
 
import json
from pandas import json_normalize
from datetime import datetime
 

def _process_doctor(ti):
    results = ti.xcom_pull(task_ids="extract_doctor")
    results = results['data']
    print(results)
    cols = ['name', 'speciality_name', 'price', 'booking_count',
            'rating_percentage', 'total_rating', 'hospital_name']
    results = json_normalize(results)[cols]
    results.to_csv('/tmp/processed_doctor.csv', index=None, header=False, sep=';')
 

def _store_doctor():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY doctors FROM stdin WITH DELIMITER as ';'",
        filename='/tmp/processed_doctor.csv'
    )
 

with DAG('doctor_processing', start_date=datetime(2023, 1, 1), 
        schedule_interval='@daily', catchup=False) as dag:
 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS doctors (
                name TEXT NOT NULL,
                speciality_name TEXT NOT NULL,
                price TEXT NOT NULL,
                booking_count INT NOT NULL,
                rating_percentage TEXT NOT NULL,
                total_rating INT NOT NULL,
                hospital_name TEXT NOT NULL
            );
        '''
    )
 
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='doctor_api', # HOST : https://www.alodokter.com/
        endpoint='api/doctors/get_listing_doctor?speciality_permalink=dokter-anak&sub_speciality_permalink=&category=speciality&start_time=&end_time=&days%5B%5D=&toggle_day=false&page=1&city_permalink=&district_permalink=&lat=&long=&limit=10&_user_id='
    )
 
    extract_doctor = SimpleHttpOperator(
        task_id='extract_doctor',
        http_conn_id='doctor_api',
        endpoint='api/doctors/get_listing_doctor?speciality_permalink=dokter-anak&sub_speciality_permalink=&category=speciality&start_time=&end_time=&days%5B%5D=&toggle_day=false&page=1&city_permalink=&district_permalink=&lat=&long=&limit=10&_user_id=',
        method='GET',
        response_filter=lambda response: json.loads(response.text), 
        log_response=True
    )
 
    process_doctor = PythonOperator(
        task_id='process_doctor',
        python_callable=_process_doctor
    )
 
    store_doctor = PythonOperator(
        task_id='store_doctor',
        python_callable=_store_doctor
    )
 
    create_table >> is_api_available >> extract_doctor >> process_doctor >> store_doctor