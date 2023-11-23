# #UN CODE QUI MARCHE 
# from datetime import datetime
# import os
# import pandas as pd
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook

# def extract_data():
#     # Spécifiez le chemin vers votre fichier CSV
#     csv_file_path = os.path.expandvars("${AIRFLOW_HOME}/data/donnees-urgences-SOS-medecins.csv")
    
#     # Chargez les données depuis le fichier CSV
#     df = pd.read_csv(csv_file_path, dtype="unicode",  sep=",")
#     print (df)

    
    
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False
# }

# with DAG(
#     'Projet12',
#     default_args=default_args,
#     schedule_interval=None,
#     start_date=datetime(2021, 1, 1),
#     catchup=False,
# ) as dag:
#     extract = PythonOperator(
#         task_id='Extract',
#         python_callable=extract_data
#     )

   


#     extract


# #chargement 
#     #postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connexion")
#     #df_departements.to_sql('Departements', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000)

#    # postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connexion")
#    # df_urgences.to_sql('Urgences', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000)

#     #postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connexion")
#     #df_age.to_sql('Age', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000)


# CREATE TABLE IF NOT EXISTS "Urgences"(
#     dep VARCHAR(255),
#     sursaud_cl_age_corona FLOAT, 
#     nbre_pass_corona FLOAT, 
#     nbre_pass_tot FLOAT, 
#     nbre_hospit_corona FLOAT,
#     nbre_pass_corona_h FLOAT, 
#     nbre_pass_corona_f FLOAT, 
#     nbre_pass_tot_h FLOAT, 
#     nbre_pass_tot_f FLOAT,
#     nbre_hospit_corona_h FLOAT, 
#     nbre_hospit_corona_f FLOAT, 
#     nbre_acte_corona FLOAT, 
#     nbre_acte_tot FLOAT,
#     nbre_acte_corona_h FLOAT, 
#     nbre_acte_corona_f FLOAT, 
#     nbre_acte_tot_h FLOAT, 
#     nbre_acte_tot_f FLOAT
# );

#   CREATE TABLE IF NOT EXISTS "Departements"
# (
#     num_dep VARCHAR(255),
#     dep_name VARCHAR(255),
#     region_name  VARCHAR(255)
# );

# CREATE TABLE IF NOT EXISTS "Age"
# (
#    code_age VARCHAR(255),
#    age VARCHAR(255)
# );

# SELECT *
# FROM "Urgences"
# INNER JOIN "Departements" ON "Urgences".dep = "Departements".num_dep;
