import datetime as dt
from datetime import timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'harun',
    'start_date': dt.datetime(2024, 10, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_data():
    '''
    Fungsi ini digunakan untuk memanggil data dari PostgreSQL
    '''
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    
    try:
        conn = db.connect(conn_string)
    except db.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return

    try:
        df = pd.read_sql("SELECT * FROM public.table_m3", conn)
        df.to_csv('/opt/airflow/dags/P2M3_Harun_data_raw.csv', index=False)
        logger.info("-------Success------")
    except Exception as e:
        logger.error(f"Error executing query: {e}")
    finally:
        conn.close()

def clean_data():
    '''
    Fungsi untuk menghilangkan missing value
    '''
    try:
        df = pd.read_csv('/opt/airflow/dags/P2M3_Harun_data_raw.csv')
    except FileNotFoundError:
        logger.error("File tidak ditemukan, pastikan file sudah ada.")
        return
    
    df = df.drop_duplicates()
    
    if 'Order_Date' in df.columns: 
        df['Order_Date'] = pd.to_datetime(df['Order_Date'], format='%d/%m/%Y', errors='coerce')
    if 'Ship_Date' in df.columns: 
        df['Ship_Date'] = pd.to_datetime(df['Ship_Date'], format='%d/%m/%Y', errors='coerce')

    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(r'\W', '', regex=True)

    # Menghapus baris yang memiliki nilai kosong
    df.dropna(inplace=True)
    logger.info(f"Jumlah baris setelah menghapus missing values: {len(df)}")


    df.to_csv('/opt/airflow/dags/P2M3_Harun_data_clean.csv', index=False)

def post_elastic():
    es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])

    if not es.ping():
        logger.error("Gagal terhubung ke Elasticsearch")
        return
    else:
        logger.info("Koneksi ke Elasticsearch berhasil")

    df_clean = pd.read_csv('/opt/airflow/dags/P2M3_Harun_data_clean.csv')

    for i, row in df_clean.iterrows():
        doc = row.to_dict()
        es.index(index="store", id=i + 1, body=doc)  # ID yang unik

    logger.info("Data berhasil dikirim ke Elasticsearch")

# DAG definition
with DAG('DATADAG',
         default_args=default_args,
         schedule_interval='30 6 * * *',
         catchup=False
         ) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data
    )

    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=clean_data
    )

    post_elastic_search = PythonOperator(
        task_id='post_elastic_search',
        python_callable=post_elastic
    )

# Set task dependency
fetch_data_task >> cleaning_data >> post_elastic_search
