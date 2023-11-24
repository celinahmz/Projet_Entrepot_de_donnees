
# Projet_Entrepot_de_donnees("EPSILON")
Projet de Modélisation d'Entrepôt de Données pour les Urgences COVID-19 en France

Introduction :
Le projet vise à créer un entrepôt de données pour analyser les urgences liées au COVID-19 en France. À partir de fichiers sources, l'objectif est de modéliser un schéma en étoile pour répondre à des questions spécifiques et faciliter l'exploration des données.

Objectifs du Projet :
1- Extraction des Données :

Nous avons utilisé le fichier `projet.py` pour extraire les données des fichiers sources dans le dossier `data` (données-urgences-SOS-medecins.csv, code-tranches-dage-donnees-urgences.csv, departements-region.json, metadonnee-urgenceshos-sosmedecin-covid19-quot.csv).

2- Transformation et Nettoyage des Données :

Nous avons identifié les incohérences, les valeurs manquantes, les doublons dans les données, ainsi que les espaces inutiles. Pour maximiser la conservation des données, nous avons décidé de créer une date unique pour les lignes où la date est saisie de manière incorrecte ('1999-01-01'). De plus, pour les lignes de la table Urgences dont les "dep" sont vides, nous avons inséré une nouvelle ligne dans le dataframe `df_departements` pour ne pas perdre ces données, en attribuant la valeur 0 comme code département.

3- Création de DAGs sur Apache Airflow :

Nous avons configuré des Directed Acyclic Graphs (DAGs) dans Apache Airflow pour automatiser les tâches d'extraction, de transformation et de chargement (ETL) des données. Cela garantit une exécution régulière et efficace des processus.

4- Création des Tables dans PostgreSQL :

Nous avons utilisé le fichier SQL `create_table.sql` pour créer les tables nécessaires dans PostgreSQL via DBeaver. Nous avons créé les tables de faits et de dimensions conformément au schéma en étoile, en créant seulement 3 tables. Ceci est justifié car dans un contexte de données simples et avec des besoins d'analyse relativement directs, un modèle en étoile avec moins de tables pourrait être suffisant et bénéfique en termes de simplicité, de performance et de maintenance. De plus, ajouter une colonne ID aurait un impact financier en termes de stockage de données supplémentaire, c'est pourquoi nous n'avons pas créé 4 tables.

6- Génération de Graphiques :

Nous avons utilisé Matplotlib pour créer des graphiques représentatifs des données extraites, offrant une visualisation claire et plus efficace pour une meilleure compréhension.

7- Validation et Tests :

Pour vérifier et valider les codes, nous avons fourni des exemples de requêtes PostgreSQL avec quelques visualisations Matplotlib sur les données. De plus, dans la fonction "extract_transform_clean_load", nous avons créé des balises <try & exception> pour lever des exceptions facilitant l'identification des problèmes. La création de la base de données est détaillée dans le fichier "create_table.sql". Nous avons ajouté des commentaires dans le code pour une lecture plus claire. Pour lancer notre code, vous devez utiliser Docker et Airflow.

Conclusion :
En conclusion, ce projet a permis de mettre en place un entrepôt de données solide et fonctionnel pour analyser les urgences liées au COVID-19 en France. L'utilisation d'Apache Airflow pour automatiser les tâches ETL assure une gestion efficace des données, facilitant ainsi les futures analyses et prises de décision basées sur des données fiables et traitées.


**Merci de prendre le temps de parcourir notre documentation ! Nous vous souhaitons une excellente lecture  dans cette expérience . N'hésitez pas à nous contacter en cas de questions ou de commentaires. Bonne exploration !**




