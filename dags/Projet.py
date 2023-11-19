from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import json
from airflow.operators.postgres_operator import PostgresOperator


def extract_data():
    # Spécifiez le chemin vers votre fichier CSV
    csv_file_path_urgences = os.path.expandvars("${AIRFLOW_HOME}/data/donnees-urgences-SOS-medecins.csv")
    # Chargez les données depuis le fichier CSV
    df_urgences = pd.read_csv(csv_file_path_urgences,  delimiter=';' ) 

#FICHIER Age
    csv_file_path_age = os.path.expandvars("${AIRFLOW_HOME}/data/code-tranches-dage-donnees-urgences.csv")
    df_age = pd.read_csv(csv_file_path_age,  delimiter=";") 
    
# ...

# Ou pour appliquer la conversion à plusieurs colonnes en même temps
# Remplacez 'colonne1', 'colonne2', etc., avec les noms de vos colonnes
#df[['colonne1', 'colonne2', 'colonne3']] = df[['colonne1', 'colonne2', 'colonne3']].astype(str)

#FICHIER departements 
    json_file_path_departements = os.path.expandvars("${AIRFLOW_HOME}/data/departements-region.json")
    with open(json_file_path_departements, 'r',encoding="UTF-8") as json_file:
        data = json.load(json_file)
        df_departements = pd.DataFrame(data)
#changer le type des colonnes de departements
        df_departements['num_dep'] = df_departements['num_dep'].astype(str)
        df_departements['dep_name'] = df_departements['dep_name'].astype(str)
        df_departements['region_name'] = df_departements['region_name'].astype(str)
#Fichier urgences 
    df_urgences.columns = df_urgences.columns.str.strip() # supp des espaces 
    df_urgences = df_urgences.drop_duplicates().reset_index(drop=True) #supp des lignes en doublons :
    columns_to_convert = ['dep', 'sursaud_cl_age_corona', 'nbre_pass_corona', 'nbre_pass_tot', 'nbre_hospit_corona',
                        'nbre_pass_corona_h', 'nbre_pass_corona_f', 'nbre_pass_tot_h', 'nbre_pass_tot_f',
                       'nbre_hospit_corona_h', 'nbre_hospit_corona_f', 'nbre_acte_corona', 'nbre_acte_tot',
                      'nbre_acte_corona_h', 'nbre_acte_corona_f', 'nbre_acte_tot_h', 'nbre_acte_tot_f']
    df_urgences[columns_to_convert] = df_urgences[columns_to_convert].apply(pd.to_numeric, errors='coerce')
    df_urgences[columns_to_convert] = df_urgences[columns_to_convert].fillna(0)  
     #Remplacer df_urgences['date_de_passage'] par l'accès à la première colonne par position
    df_urgences[df_urgences.columns[1]] = pd.to_datetime(df_urgences[df_urgences.columns[1]], errors='coerce')
     #pour le nettoyages des dates soit on va les supprimer ou on va  les laisser mais avec une date specifique : 
    df_urgences['date_de_passage'].fillna('1999-01-01', inplace=True) 
    print('hello')

#NETTOYAGE FICHIER DEPARTEMENTS
#chargement 
    
    #postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connexion")
   # df_departements.to_sql('D', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000)


    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(
    'Projet1',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    extract = PythonOperator(
        task_id='Extract',
        python_callable=extract_data
    )

    #create_table = PostgresOperator(
   # task_id='create_table',
    #postgres_conn_id='postgres_connexion',
    #sql='sql/create_table.sql'
    #)



    extract 

