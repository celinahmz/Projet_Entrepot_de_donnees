# Projet_Entrepot_de_donnees
rojet de Modélisation d'Entrepôt de Données pour les Urgences COVID-19 en France

Introduction :
Le projet vise à créer un entrepôt de données pour analyser les urgences liées au COVID-19 en France. À partir de fichiers sources, l'objectif est de modéliser un schéma en étoile pour répondre à des questions spécifiques et faciliter l'exploration des données.

Objectifs du Projet
1- Extraction des Données :

Nous avons utilisé le fichier projet.py pour extraire les données des fichiers sources dans le dossier data(donnees-urgences-SOS-medecins.csv, code-tranches-dage-donnees-urgences.csv, departements-region.json, metadonnee-urgenceshos-sosmedecin-covid19-quot.csv).

2- Transformation et Nettoyage des Données :

Nous avons Identifieé les incohérences, les valeurs manquantes , les doublons dans les données ou les espaces inutiles . Aussi pour garder le maximum possible des données  : on a décidé de creer une date unique pour  les lignes ou la dates est saisie incorrectement ('1999-01-01') .
.Pour le ligne de la table Urgences  dont les "dep" sont vides on a  inserer une nouvelle ligne dons la data frame df_departements pour mettre ne pas perdre ces lignes on a attrubuer comme code departement la  valeur 0 .

3- Création de DAGs sur Apache Airflow :

Nous avons configuré des Directed Acyclic Graphs (DAGs) dans Apache Airflow pour automatiser les tâches d'extraction, de transformation et de chargement (ETL) des données.Cela garantit une exécution régulière et efficace des processus.

4- Création des Tables dans PostgreSQL :

Nous avons utilisé le fichier SQL create_table.sql pour créer les tables nécessaires dans PostgreSQL via DBeaver.
Nous avons crée les tables de faits et de dimensions conformément au schéma en étoile,sachant qu'on a  creer que 3 tables car : 
           1/Dans un contexte de données simples et avec des besoins d'analyse relativement directs, un modèle en étoile avec moins de tables pourrait être suffisant et bénéfique en termes de simplicité, de performance et de maintenance. Cela peut être particulièrement vrai .
           2/Ajouter une colonne ID peut avoir un impact financier  en termes de stockage de données supplémentaire .C'est pourquoi nous n'abons pas creer 4 tables .


6- Génération de Graphiques :

nous  avons utlisé  Matplotlib pour créer des graphiques représentatifs des données extraites, offrant une visualisation claire et plus efficaces pour une meilleure compréhension.

7- Validation et Tests :

pour véerifier et valider les codes nous vous avons données des exepmles des requêtes Postgresql avec quelques visualisation Matplotlib sur la data .


Conclusion: 
En conclusion, ce projet a permis de mettre en place un entrepôt de données solide et fonctionnel pour analyser les urgences liées au COVID-19 en France. L'utilisation d'Apache Airflow pour automatiser les tâches ETL assure une gestion efficace des données, facilitant ainsi les futures analyses et prises de décision basées sur des données fiables et traitées.
