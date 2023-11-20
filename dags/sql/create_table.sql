CREATE TABLE IF NOT EXISTS "Urgences"(
    dep VARCHAR(255),
    sursaud_cl_age_corona VARCHAR(255), 
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
    nbre_acte_tot_f FLOAT
);

CREATE TABLE IF NOT EXISTS "Departements"
(
    num_dep VARCHAR(255),
    dep_name VARCHAR(255),
    region_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS "Age"
(
   Code_age VARCHAR(255),
   age VARCHAR(255)
);




