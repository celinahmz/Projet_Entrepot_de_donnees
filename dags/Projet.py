from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import json
from airflow.operators.postgres_operator import PostgresOperator
import numpy as np
from airflow.operators.email_operator import EmailOperator
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# Ajoutez ceci au début de votre fichier DAG pour installer Matplotlib
from airflow.operators.bash_operator import BashOperator




# Ensuite, définissez votre tâche d'extraction après l'installation






def extract_data():
    try:
        # Fichier Urgences
        csv_file_path_urgences = os.path.expandvars("${AIRFLOW_HOME}/data/donnees-urgences-SOS-medecins.csv")
        df_urgences = pd.read_csv(csv_file_path_urgences, delimiter=';')
        df_urgences.columns = df_urgences.columns.str.strip()
        df_urgences = df_urgences.drop_duplicates().reset_index(drop=True)
        columns_to_convert = ['dep', 'sursaud_cl_age_corona', 'nbre_pass_corona', 'nbre_pass_tot', 'nbre_hospit_corona',
                              'nbre_pass_corona_h', 'nbre_pass_corona_f', 'nbre_pass_tot_h', 'nbre_pass_tot_f',
                              'nbre_hospit_corona_h', 'nbre_hospit_corona_f', 'nbre_acte_corona', 'nbre_acte_tot',
                              'nbre_acte_corona_h', 'nbre_acte_corona_f', 'nbre_acte_tot_h', 'nbre_acte_tot_f']
        df_urgences[columns_to_convert] = df_urgences[columns_to_convert].apply(pd.to_numeric, errors='coerce')
        df_urgences[columns_to_convert] = df_urgences[columns_to_convert].fillna(0)
        df_urgences[df_urgences.columns[1]] = pd.to_datetime(df_urgences[df_urgences.columns[1]], errors='coerce')
        df_urgences['date_de_passage'].fillna('1999-01-01', inplace=True)

    except Exception as e_urgences:
        # Si une exception se produit pendant l'extraction des données Urgences
        print(f"Une erreur s'est produite lors de l'extraction des données Urgences : {str(e_urgences)}")
        # Vous pouvez effectuer des actions spécifiques en cas d'erreur

    try:
        # Fichier departements
        json_file_path_departements = os.path.expandvars("${AIRFLOW_HOME}/data/departements-region.json")
        with open(json_file_path_departements, 'r', encoding="UTF-8") as json_file:
            data = json.load(json_file)
            df_departements = pd.DataFrame(data)
            df_departements['num_dep'] = pd.to_numeric(df_departements['num_dep'], errors='coerce')
            df_departements['num_dep'] = df_departements['num_dep'].astype('Int64')
            df_departements['dep_name'] = df_departements['dep_name'].astype(str)
            print(len(df_departements))
            df_departements = df_departements.dropna(subset=['num_dep'])
            print(len(df_departements))

    except Exception as e_departements:
        # Si une exception se produit pendant l'extraction des données Departements
        print(f"Une erreur s'est produite lors de l'extraction des données Departements : {str(e_departements)}")
        # Vous pouvez effectuer des actions spécifiques en cas d'erreur

    try:
        # FICHIER Age
        csv_file_path_age = os.path.expandvars("${AIRFLOW_HOME}/data/code-tranches-dage-donnees-urgences.csv")
        df_age = pd.read_csv(csv_file_path_age, delimiter=";")
        df_age["Code tranches d'age"] = df_age["Code tranches d'age"].astype(int)
        df_age['Age'] = df_age['Age'].astype(str)
        print(df_age)

        postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connexion")

        df_age.rename(columns={'Code tranches d\'age': 'code_age'}, inplace=True)
        df_age.rename(columns={'Age': 'agess'}, inplace=True)
        print(df_age.columns)
        nouvelle_ligne = {'num_dep': 0, 'dep_name': 'benguerden','Région_name' :'Tunisie'}
        df_departements= df_departements.append(nouvelle_ligne, ignore_index=True)
    except Exception as e_age:
        # Si une exception se produit pendant l'extraction des données Age
        print(f"Une erreur s'est produite lors de l'extraction des données Age : {str(e_age)}")
        # Vous pouvez effectuer des actions spécifiques en cas d'erreur
    try:



        # Alimentation de l'entrepôt
        postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connexion")

        # Chargement des données dans la table 'Departements'
        df_departements.to_sql('Departements', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='append',
                               chunksize=1000, index=False)

        # Chargement des données dans la table 'Age'
        df_age.to_sql('Ages', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='append', chunksize=1000, index=False)

        # Chargement des données dans la table 'Urgences'
        df_urgences.to_sql('Urgences', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='append', chunksize=1000,
                           index=False)

        # Exécution de la requête SELECT * FROM Urgences;
        print('########################################################################')
        print('#                                                                      #')
        print("#   ce dessous est la visualisation du contenu de la table Urgences :  #")
        print('#                                                                      #')
        print('########################################################################')

        show_tables_query = "SELECT * FROM \"Urgences\";"
        tables = postgres_sql_upload.get_pandas_df(show_tables_query)
        print(tables)

        print('========================')


    except Exception as e:
        # Si une exception se produit, imprimez l'erreur et faites toute autre action souhaitée
        print(f"Une erreur s'est produite : {str(e)}")
        # Vous pouvez également lever une exception ici si vous voulez arrêter l'exécution du DAG

#     destinataire = 'filali.dia18@example.com'
#     sujet = 'Test d\'envoi d\'e-mail avec Python'
#     corps = 'Ceci est un e-mail de test envoyé depuis Python.'
# #    # Informations de connexion au serveur SMTP
#     serveur_smtp = 'smtp.gmail.com'
#     port_smtp = 587
#     utilisateur_smtp = 'celinahmz@example.com'
#     mot_de_passe_smtp = ''

#     # Création de l'objet MIMEMultipart
#     message = MIMEMultipart()
#     message['From'] = utilisateur_smtp
#     message['To'] = destinataire
#     message['Subject'] = sujet

#     # Ajout du corps du message
#     message.attach(MIMEText(corps, 'plain'))

#     # Initialisation de la connexion SMTP
#     serveur = smtplib.SMTP(serveur_smtp, port_smtp)
#     serveur.starttls()

#     # Connexion au serveur SMTP avec l'adresse e-mail de l'expéditeur et le mot de passe
#     serveur.login(utilisateur_smtp, mot_de_passe_smtp)

#     # Envoi de l'e-mail
#     serveur.sendmail(utilisateur_smtp, destinataire, message.as_string())

#     # Fermeture de la connexion SMTP
#     serveur.quit()

# # Exemple d'utilisation



    

# Le reste du code reste inchangé


    # Récupérer les données nécessaires de la base de données ou d'où vous les avez
    # Exemple : récupérer les données de la table 'Urgences'
    # Données fictives
    x = [1, 2, 3, 4, 5]
    y = [2, 4, 6, 8, 10]    
    import matplotlib.pyplot as plt

    # Création de la courbe
    plt.plot(x, y, label='Ma Courbe')   

    # Ajout de titres et d'étiquettes
    plt.title('Exemple de Courbe avec Matplotlib')
    plt.xlabel('Axe X')
    plt.ylabel('Axe Y') 

    # Ajout d'une légende
    plt.legend()    

    # Affichage de la courbe
    plt.show()

    # Vous pouvez également afficher le graphique avec plt.show() si vous le souhaitez
    # plt.show()



# Configurations du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}
with DAG(
    'Projet1',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_connexion',
        sql='sql/create_table.sql'
    )
    
    install_matplotlib = BashOperator(
        task_id='install_matplotlib',
        bash_command='pip install matplotlib',
        dag=dag,
    )

    extract = PythonOperator(
        task_id='Extract',
        python_callable=extract_data
    )
# Assurez-vous de définir la dépendance appropriée
create_table >>install_matplotlib >> extract


#     email_on_success = EmailOperator(
#         task_id='send_email_on_success',
#         to='filali.dia18@gmail.com',  # Remplacez par votre adresse e-mail
#         subject='Succès du DAG',
#         html_content='Le DAG a réussi.',
#         dag=dag,
#         trigger_rule='all_success',
#     )

#     email_on_failure = EmailOperator(
#         task_id='send_email_on_failure',
#         to='filali.dia18@gmail.com',  # Remplacez par votre adresse e-mail
#         subject='Échec du DAG',
#         html_content='Le DAG a échoué. Veuillez vérifier les journaux.',
#         dag=dag,
#         trigger_rule='all_failed',
#     )

#     create_table >> extract >> [email_on_success, email_on_failure]


# with DAG(
#     'Projet1',
#     default_args=default_args,
#     schedule_interval=None,
#     start_date=datetime(2021, 1, 1),
#     catchup=False,
# ) as dag:
#     # Opérateur d'extraction des données
#     extract = PythonOperator(
#         task_id='Extract',
#         python_callable=extract_data,
#         provide_context=True,  # Ceci est important pour accéder au contexte des tâches
#     )

    

    # Vous pouvez également ajouter d'autres tâches et dépendances si nécessaire

    # Définir les dépendances entre les tâches


# with DAG(
#     'Projet1',
#     default_args=default_args,
#     schedule_interval=None,
#     start_date=datetime(2021, 1, 1),
#     catchup=False,
# ) as dag:
#     extract = PythonOperator(
#         task_id='Extract',
#         python_callable=extract_data
#     )

#     create_table = PostgresOperator(
#     task_id='create_table',
#     postgres_conn_id='postgres_connexion',
#     sql='sql/create_table.sql'
#     )
   
#     # join_table = PostgresOperator(
#     # task_id='join_table',
#     # postgres_conn_id='postgres_connexion',
#     # sql='sql/join_table.sql'
#     # )

#     # Exemple de tâche EmailOperator (envoie un e-mail en cas d'échec)
#     # email_on_failure = EmailOperator(
#     # task_id='send_email_on_failure',
#     # to='celinahmz@gmail.com',
#     # subject='Échec du DAG',
#     # html_content='Le DAG a échoué. Veuillez vérifier les journaux.',
#     # dag=dag,
#     # trigger_rule='all_failed',  # Cette tâche s'exécutera uniquement en cas d'échec de toutes les tâches précédentes
#     # )

#     # Exemple de tâche EmailOperator (envoie un e-mail en cas de réussite)
#     # email_on_success = EmailOperator(
#     # task_id='send_email_on_success',
#     # to='celinahmz@gmail.com',
#     # subject='Succès du DAG',
#     # html_content='Le DAG a réussi.',
#     # dag=dag,
#     # trigger_rule='all_success',  # Cette tâche s'exécutera uniquement en cas de réussite de toutes les tâches précédentes
#     # )

    



# create_table >> extract   #>> join_table

