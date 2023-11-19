#UN CODE QUI MARCHE 
from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def extract_data():
    # Spécifiez le chemin vers votre fichier CSV
    csv_file_path = os.path.expandvars("${AIRFLOW_HOME}/data/donnees-urgences-SOS-medecins.csv")
    
    # Chargez les données depuis le fichier CSV
    df = pd.read_csv(csv_file_path, dtype="unicode",  sep=",")
    print (df)

    
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(
    'Projet12',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    extract = PythonOperator(
        task_id='Extract',
        python_callable=extract_data
    )

   


    extract
