import psycopg2
import os
import logging

#importing the module for preprocessing the data
import func.sql_preproc

logging.basicConfig()
logger = logging.getLogger("SQL_upload")
logger.setLevel(logging.INFO)

#writing the SQL code needed
#creating the tables in postgres
sql_create = '''

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

#loading data in the tables
sql_load = '''

COPY assessments(
    code_module,
    code_presentation,
    id_assessment,
    assessment_type, 
    date,
    weight
    )
FROM 'D:/Thesis/Work/github/event-log-partitioning/data/assessments_clean.csv'
DELIMITER ','
CSV HEADER;

COPY vle(
    id_site,
    code_module,
    code_presentation,
    activity_type
    )
FROM 'D:/Thesis/Work/github/event-log-partitioning/data/vle_clean.csv'
DELIMITER ','
CSV HEADER;

COPY student_assessment(
    id_assessment,
    id_student, 
    is_banked,
    score
    )
FROM 'D:/Thesis/Work/github/event-log-partitioning/data/studentAssessment_clean.csv'
DELIMITER ','
CSV HEADER;

COPY registration(
    code_module,
    code_presentation,
    id_student, 
    date_registration,
    date_unregistration
    )
FROM 'D:/Thesis/Work/github/event-log-partitioning/data/studentRegistration_clean.csv'
DELIMITER ','
CSV HEADER;

'''

def postgres_connect ():
    ''' A function that connects to postgres 
        with my credentials

        Returns: 
            Returns the conn object to be 
            used to load tables
    
    '''
    #connecting to postgres
    conn = psycopg2.connect(database="OULAD",
                            user='postgres', password='thesis1', 
                            host='127.0.0.1', port='5433'
    
    )
    
    conn.autocommit = True

    logger.info("Postgres Connection Successfull")

    return conn

#creating the sql tables
conn = postgres_connect()

#running the SQL Querry
conn.cursor().execute(sql_create)
conn.cursor().execute(sql_load)
logger.info("Data was sent to Postgres")

#closing the SQl connection
conn.commit()
conn.close()