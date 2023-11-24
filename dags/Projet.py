from datetime import datetime , timedelta
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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator





def extract_data():
   try:
       # Fichier Urgences
       csv_file_path_urgences = os.path.expandvars("${AIRFLOW_HOME}/data/donnees-urgences-SOS-medecins.csv")
       df_urgences = pd.read_csv(csv_file_path_urgences, delimiter=';')
       df_urgences.columns = df_urgences.columns.str.strip()
       df_urgences = df_urgences.drop_duplicates().reset_index(drop=True)
       columns_to_convert = [ 'sursaud_cl_age_corona', 'nbre_pass_corona', 'nbre_pass_tot', 'nbre_hospit_corona',
                             'nbre_pass_corona_h', 'nbre_pass_corona_f', 'nbre_pass_tot_h', 'nbre_pass_tot_f',
                             'nbre_hospit_corona_h', 'nbre_hospit_corona_f', 'nbre_acte_corona', 'nbre_acte_tot',
                             'nbre_acte_corona_h', 'nbre_acte_corona_f', 'nbre_acte_tot_h', 'nbre_acte_tot_f']
       df_urgences[columns_to_convert] = df_urgences[columns_to_convert].apply(pd.to_numeric, errors='coerce')
       df_urgences[columns_to_convert] = df_urgences[columns_to_convert].fillna(0)
       df_urgences[df_urgences.columns[1]] = pd.to_datetime(df_urgences[df_urgences.columns[1]], errors='coerce')
       df_urgences['date_de_passage'].fillna('1999-01-01', inplace=True)
       df_urgences['dep'] = df_urgences['dep'].astype(str)
       df_urgences['dep'] = df_urgences['dep'].apply(lambda x: '0' + x if len(x) < 2 else x)
              # Exécution de la requête SELECT * FROM Urgences;
       print('########################################################################')
       print('#                                                                      #')
       print("#   ce dessous est la visualisation du contenu de la table Urgences :  #")
       print('#                                                                      #')
       print('########################################################################')
       print(df_urgences)
        
       


   except Exception as e_urgences:
       # Si une exception se produit pendant l'extraction des données Urgences
       print(f"Une erreur s'est produite lors de l'extraction des données Urgences : {str(e_urgences)}")


   try:
       # Fichier departements
       json_file_path_departements = os.path.expandvars("${AIRFLOW_HOME}/data/departements-region.json")
       with open(json_file_path_departements, 'r', encoding="UTF-8") as json_file:
           data = json.load(json_file)
           df_departements = pd.DataFrame(data)
           #df_departements['num_dep'] = pd.to_numeric(df_departements['num_dep'], errors='coerce')
           df_departements['num_dep'] = df_departements['num_dep'].astype(str)
           df_departements['dep_name'] = df_departements['dep_name'].astype(str)
           df_departements = df_departements.dropna(subset=['num_dep'])
       print('########################################################################')
       print('#                                                                      #')
       print("#   ce dessous est la visualisation du contenu de la table Urgences :  #")
       print('#                                                                      #')
       print('########################################################################')
       print(df_departements)



   except Exception as e_departements:
       # Si une exception se produit pendant l'extraction des données Departements
       print(f"Une erreur s'est produite lors de l'extraction des données Departements : {str(e_departements)}")


   try:
       # FICHIER Age
       csv_file_path_age = os.path.expandvars("${AIRFLOW_HOME}/data/code-tranches-dage-donnees-urgences.csv")
       df_age = pd.read_csv(csv_file_path_age, delimiter=";")
       df_age["Code tranches d'age"] = df_age["Code tranches d'age"].astype(int)
       df_age['Age'] = df_age['Age'].astype(str)
       postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connexion")
       df_age.rename(columns={'Code tranches d\'age': 'code_age'}, inplace=True)
       df_age.rename(columns={'Age': 'agess'}, inplace=True)
       print(df_age.columns)
       nouvelle_ligne = {'num_dep': '0', 'dep_name': 'benguerden','Région_name' :'Tunisie'}
       df_departements= df_departements.append(nouvelle_ligne, ignore_index=True)
       print('########################################################################')
       print('#                                                                      #')
       print("#   ce dessous est la visualisation du contenu de la table Urgences :  #")
       print('#                                                                      #')
       print('########################################################################')
       print(df_age)


   except Exception as e_age:
       # Si une exception se produit pendant l'extraction des données Age
       print(f"Une erreur s'est produite lors de l'extraction des données Age : {str(e_age)}")
   try:
        ########################################################################
        #                                                                      #
        #   ci dessous  il y'a le bloc d'alimentation des données des la BDD : #
        #                                                                      #
        ########################################################################
 



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
        ########################################################################
        #                                                                      #
        #  Ci dessous  il y'a le bloc d'excution des requêtes  SQL  de la BDD #
        #  et quelques exemples de visualisation des données avec matplotlib: #
        #                                                                      #
        ########################################################################
       import matplotlib.pyplot as plt

        #nbre des contaminés covid homme et femme separé par departement 
       Show1 = "SELECT sum(nbre_pass_corona_h) as h, sum(nbre_pass_corona_f) as f ,dep FROM \"Urgences\"   group by dep;"
       Show2 = "SELECT sum(nbre_pass_corona_h) as h, sum(nbre_pass_corona_f) as f ,sursaud_cl_age_corona as s FROM \"Urgences\"   group by sursaud_cl_age_corona;"
       Show3 = "SELECT sum(t1.nbre_pass_corona) as t, t2.region_name as s  FROM \"Urgences\" t1  join \"Departements\" t2 on t1.dep = t2.num_dep group by t2.region_name;"       


       ########################################################################################################################
       # 1 - Total Nombre de passages aux urgences pour suspicion de COVID-19 hommes et femmes par numéro de departement
       ########################################################################################################################
       tables = postgres_sql_upload.get_pandas_df(Show1)
       chemin= os.path.expandvars("${AIRFLOW_HOME}/data/")

       x= tables['dep']
       y = tables['h']
       z = tables['f']      

       # Création de la figure avec une grille 1x1
       plt.figure(figsize=(10, 8))      

       # Création de la première sous-figure (graphique)
       plt.subplot(111)     

       # Tracer la courbe pour les hommes (Ma Courbe)
       plt.plot(x, y, label='Homme')        

       # Tracer la courbe pour les femmes
       plt.plot(x, z, label='Femme')        

       # Ajout de titres et d'étiquettes
       plt.title('Nombre de passages aux urgences pour suspicion de COVID-19 par département ')
       plt.xlabel('Département')
       plt.ylabel('Total de passages au urgences de COVID-19')
       # Ajout de la légende
       plt.legend()
       # Affichage de la courbe
       plt.show()
       chemin =chemin +"Nombre_de_passages_aux_urgences_pour_suspicion_COVID-19_par_num_dep.png"
       plt.savefig(chemin)
       ########################################################################################################################
       #2 Total Nombre de passages aux urgences pour suspicion de COVID-19 hommes et femmes par code d'age
       ########################################################################################################################
       chemin= os.path.expandvars("${AIRFLOW_HOME}/data/")
       tables = postgres_sql_upload.get_pandas_df(Show2)
       x= tables['s']
       y = tables['h']
       z = tables['f']      

       # Création de la figure avec une grille 1x1
       plt.figure(figsize=(10, 8))      

       # Création de la première sous-figure (graphique)
       plt.subplot(111)     

       # Tracer la courbe pour les hommes (Ma Courbe)
       plt.plot(x, y, label='Homme')        

       # Tracer la courbe pour les femmes
       plt.plot(x, z, label='Femme')        

       # Ajout de titres et d'étiquettes
       plt.title('Nombre de passages aux urgences pour suspicion de COVID-19 par code age')
       plt.xlabel('Département')
       plt.ylabel('Total de passages suspiction de COVID-19')
       # Ajout de la légende
       plt.legend()
       # Affichage de la courbe
       plt.show()
       chemin =chemin +"Nombre_de_passages_aux_urgences_pour_suspicion_COVID-19_par_code_age.png"
       plt.savefig(chemin)
        ########################################################################
        #3 pourcentage  des contaminés COVID-19 hommes et femmes par depatement
        ########################################################################
       chemin= os.path.expandvars("${AIRFLOW_HOME}/data/")
       tables = postgres_sql_upload.get_pandas_df(Show3)
       xi = tables['s']
       yz = tables['t']   
       plt.figure(figsize=(13, 13))
       # Création de la courbe
       plt.pie(yz, labels=xi , autopct='%.2f%%',pctdistance=1)
       plt.title('Nombre_de_passages_aux_urgences_pour_suspicion_COVID-19_par_région')
       # Affichage de la courbe
       plt.show()
       chemin =chemin +"Nombre_de_passages_aux_urgences_pour_suspicion_COVID-19_par_région.png"
       plt.savefig(chemin)
   except Exception as e:
            # Si une exception se produit, imprimez l'erreur et faites toute autre action souhaitée
            print(f"Une erreur s'est produite : {str(e)}")





   
#------------------------------------------------------------------------------------------------------------------------
#     destinataire = 'filali.dia18@example.com'
#     sujet = 'Test d\'envoi d\'e-mail avec Python'
#     corps = 'Ceci est un e-mail de test envoyé depuis Python.'
# #    # Informations de connexion au serveur SMTP
#     serveur_smtp = 'smtp.gmail.com'
#     port_smtp = 587
#     utilisateur_smtp = 'celinahmz@example.com'
#     mot_de_passe_smtp = ''
#     email_subject = "mail de confiramtion "
#     email_body = "Le DAG a bien marché behy !"


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
#------------------------------------------------------------------------------------------------------------------------




# Configurations du DAG
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email': ['filali.dia18@gmail.com'],
   'email_on_failure': True,
   'email_on_retry': True,
   'retries': 1,
   'retry_delay': timedelta(seconds=5),
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


   extract_transform_clean_load = PythonOperator(
       task_id='extract_transform_clean_load',
       python_callable=extract_data
   )
#    email_task = EmailOperator(
#    task_id='send_success_email',
#    to='filali.dia18@gmail.com',  
#    subject=email_subject,
#    html_content=email_body,
# )


# Assurez-vous de définir la dépendance appropriée
create_table >>install_matplotlib >> extract_transform_clean_load

