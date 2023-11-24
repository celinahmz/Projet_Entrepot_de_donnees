-- Suppression des tables s'ils existent
DROP TABLE IF EXISTS "Urgences";
DROP TABLE IF EXISTS "Ages";
DROP TABLE IF EXISTS "Departements";


-- Création des tables


CREATE TABLE "Departements"
(
   num_dep VARCHAR(255) PRIMARY KEY,
   dep_name VARCHAR(255),
   region_name VARCHAR(255)
);


CREATE TABLE "Ages"
(
   Code_age integer PRIMARY KEY,
   Agess VARCHAR(255)
);


CREATE TABLE "Urgences"
(
   dep VARCHAR(255),
   date_de_passage date ,
   sursaud_cl_age_corona integer ,
   nbre_pass_corona FLOAT,
   nbre_pass_tot FLOAT,
   nbre_hospit_corona FLOAT,
   nbre_pass_corona_h FLOAT,
   nbre_pass_corona_f FLOAT,
   nbre_pass_tot_h FLOAT,
   nbre_pass_tot_f FLOAT,
   nbre_hospit_corona_h FLOAT,
   nbre_hospit_corona_f FLOAT,
   nbre_acte_corona FLOAT,
   nbre_acte_tot FLOAT,
   nbre_acte_corona_h FLOAT,
   nbre_acte_corona_f FLOAT,
   nbre_acte_tot_h FLOAT,
   nbre_acte_tot_f FLOAT,
   PRIMARY key(dep , date_de_passage , sursaud_cl_age_corona) 
);


-- Ajout des contraintes de clé étrangère
ALTER TABLE "Urgences" ADD CONSTRAINT Urgences_dep_fkey FOREIGN KEY (dep) REFERENCES "Departements"(num_dep);
ALTER TABLE "Urgences" ADD CONSTRAINT Urgences_age_fkey FOREIGN KEY (sursaud_cl_age_corona) REFERENCES "Ages"(Code_age);






















































-- -- Suppression des tables s'ils existent
-- DROP TABLE IF EXISTS "Urgence";
-- DROP TABLE IF EXISTS "Age";
-- DROP TABLE IF EXISTS "Departement";


-- -- -- Création des tables

-- CREATE TABLE "Departement"
-- (
--     num_dep VARCHAR(255) PRIMARY KEY,
--     dep_name VARCHAR(255),
--     region_name VARCHAR(255)
-- );

-- CREATE TABLE "Age"
-- (
--     Code_age integer PRIMARY KEY,
--     Agess VARCHAR(255)
-- );

-- -- -- CREATE TABLE "passage_urgence"
-- -- -- (
-- -- --    num_dep 
-- -- --    DATA
-- -- --    tranche_age
-- -- --    );

-- -- -- CREATE TABLE "hospitalisation"
-- -- --    num_dep
-- -- --    date
-- -- --    );


-- CREATE TABLE "Urgence"
-- (
--     dep VARCHAR(255),
--     date_de_passage date ,
--     sursaud_cl_age_corona integer , 
--     nbre_pass_corona FLOAT, 
--     nbre_pass_tot FLOAT, 
--     nbre_hospit_corona FLOAT,
--     nbre_pass_corona_h FLOAT, 
--     nbre_pass_corona_f FLOAT, 
--     nbre_pass_tot_h FLOAT, 
--     nbre_pass_tot_f FLOAT,
--     nbre_hospit_corona_h FLOAT, 
--     nbre_hospit_corona_f FLOAT, 
--     nbre_acte_corona FLOAT, 
--     nbre_acte_tot FLOAT,
--     nbre_acte_corona_h FLOAT, 
--     nbre_acte_corona_f FLOAT, 
--     nbre_acte_tot_h FLOAT, 
--     nbre_acte_tot_f FLOAT
-- );

-- -- -- Ajout des contraintes de clé étrangère
-- ALTER TABLE "Urgence" ADD CONSTRAINT Urgence_dep_fkey FOREIGN KEY (dep) REFERENCES "Departement"(num_dep);
-- ALTER TABLE "Urgence" ADD CONSTRAINT Urgence_age_fkey FOREIGN KEY (sursaud_cl_age_corona) REFERENCES "Age"(Code_age);

