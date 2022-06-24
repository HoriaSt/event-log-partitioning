import psycopg2
import os

#running the preproc file which prepares the csv for SQL upload
os.system("sql_preproc.py 1")

#connecting to postgres
conn = psycopg2.connect(database="OULAD",
                        user='postgres', password='thesis1', 
                        host='127.0.0.1', port='5433'
)
  
conn.autocommit = True
cursor = conn.cursor()

#creating the sql tables
sql = '''

CREATE TABLE IF NOT EXISTS assessments (
    code_module char(3) NOT NULL,
    code_presentation char(5) NOT NULL,
    id_assessment varchar NOT NULL,
    assessment_type varchar NOT NULL, 
    date date,
    weight float
);

CREATE TABLE IF NOT EXISTS vle (
    id_site int NOT NULL,
    code_module char(3) NOT NULL,
    code_presentation char(5) NOT NULL,
    activity_type varchar
);

CREATE TABLE IF NOT EXISTS student_assessment (
    id_assessment varchar NOT NULL,
    id_student varchar NOT NULL, 
    is_banked bool,
    score float
);

CREATE TABLE IF NOT EXISTS registration (
    code_module varchar NOT NULL,
    code_presentation char(5) NOT NULL,
    id_student varchar NOT NULL, 
    date_registration date,
    date_unregistration boolean
);
'''
cursor.execute(sql)
  

#loading data in the table 
sql2 = '''

COPY assessments(
    code_module,
    code_presentation,
    id_assessment,
    assessment_type, 
    date,
    weight
    )
FROM '../data/assessments_clean.csv'
DELIMITER ','
CSV HEADER;

COPY vle(
    id_site,
    code_module,
    code_presentation,
    activity_type
    )
FROM '../data/vle_clean.csv'
DELIMITER ','
CSV HEADER;

COPY student_assessment(
    id_assessment,
    id_student, 
    is_banked,
    score
    )
FROM '../data/studentAssessment_clean.csv'
DELIMITER ','
CSV HEADER;

COPY registration(
    code_module,
    code_presentation,
    id_student, 
    date_registration,
    date_unregistration
    )
FROM '../data/studentRegistration_clean.csv'
DELIMITER ','
CSV HEADER;

'''
cursor.execute(sql2)

#closing the SQl connection
conn.commit()
conn.close()