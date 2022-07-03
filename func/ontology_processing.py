import pandas as pd
import pm4py
import psycopg2
import logging

from func.event_log import case_id

def ontology_import (query=str):
    ''' Ontology_import function reads from
        a PostgreSQL database into pandas

        Args: 
            query: the SQL query to be used
        
        Returns:
            a pandas dataframe containing the
            ontology artifact
    
    '''
    logger = logging.getLogger("SQL_read_in")
    logger.setLevel(logging.DEBUG)
    from psycopg2 import Error

    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                    password="thesis1",
                                    host="localhost",
                                    port="5433",
                                    database="OULAD")

        # Create a cursor to perform database operations
        cursor = connection.cursor()

        logger.debug("You are connected to - %s", cursor)

        #getting the SQL info
        cursor.execute(query)
        grades = cursor.fetchall()

        logger.info("Succesfully read-in the SQL data")



    except (Exception, Error) as error:
        logger.warning("Error while connecting to PostgreSQL: %s", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            logger.debug("PostgreSQL connection is closed")
    return grades

def domain_knowledge_processing_partitioning (query):
    ''' This function is dataset specific and performs all needed
    operations for preparing the cases from OULAD data for selection.
    This function is tailored for the semi-automatic approach

    The function calls an ontology_import function which loads the ontology
    from SQL. Afterwards, the data imported gets preprocessed and prepared 
    for the split. Student assignments get grouped together and the final 
    grade of a student in the course get summed up. The function also detects
    if a student dropped out or not.

    Args:
        query: 
            the SQL query for extracting domain knowledge from
            the ontology

    Returns:
        A pd dataframe which includes all the needed information
        for selecting the wanted and unwanted traces using a 
        rule defined by domain experts.

        An example is:

            case:concept:name	score	still_enrolled
        0	100561_DDD_2014J	72.50	True
        1	1006742_FFF_2014B	9.75	True

        still_enrolled: shows if a student dropped out
        score: shows the final grade of the student
        case:concept:name: case identifier
    '''
    logger = logging.getLogger("Domain_Knowledge_Manipulation")
    logger.setLevel(logging.DEBUG)    
    
    #importing the domain knowledge information
    grades = ontology_import(query = query)

    logger.debug("Ontology was loaded and has a length of %s",len(grades))

    #converting to pd and dealing with dtypes
    grades = pd.DataFrame(grades, columns = ["code_module","code_presentation","id_student","id_assessment","weight","score","still_enrolled"])
    grades["id_assessment"]=pd.to_numeric(grades["id_assessment"])  
    
    logger.debug("Creating new event_id for grades")
    
    keys = ["id_student", "code_module", "code_presentation"]
    
    dummy=[]
    for i,rows in grades.iterrows():
        #creating the case_id
        dummy.append ( case_id(rows, keys) )

    grades["case:concept:name"] = dummy
    
    #calculating the score for each assignment
    grades["score"] = grades["score"]*grades["weight"]/100
    grades.drop(["code_module","code_presentation","id_student","weight","id_assessment"],axis=1, inplace = True)

    logger.debug("Assignment score is calculated: %s", grades.head(3))
    
    #keeping only the needed columns
    grades = grades[["case:concept:name","score","still_enrolled"]]
    grades.reset_index(drop=True,inplace=True)
    grades["count"] = grades["case:concept:name"].copy()


    #grouping for getting a final grade/student
    grades = grades.groupby(["case:concept:name"]).agg({ "score":"sum",
                                                     "still_enrolled":"sum",
                                                     "count": "count" })

    logger.debug("Final grades / student obtained: %s",grades.head(3))

    #checking if a student dropped out or not
    still_enrolled = []
    for i,rows in grades.iterrows() :
        if rows["still_enrolled"] == rows["count"]:
            still_enrolled.append(True)
        else: still_enrolled.append(False)
    grades["still_enrolled"] = still_enrolled

    grades.reset_index(inplace = True)
    grades.drop("count",axis=1, inplace = True)

    logger.debug("The domain knowledge was processed: %s", grades.head(3))

    return grades

def domain_knowledge_clustering (query_grades, query_vle):
    from psycopg2 import Error

    logger = logging.getLogger("SQL_read_in")
    logger.setLevel(logging.DEBUG)

    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                    password="thesis1",
                                    host="localhost",
                                    port="5433",
                                    database="OULAD")

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Executing a SQL query
        cursor.execute("SELECT version();")
        
        #getting the SQL info
        cursor.execute(query_grades)
        grade_profile = cursor.fetchall()

        cursor.execute(query_vle)
        vle = cursor.fetchall()
        logger.info("Information read-in. vle has length %s and grades have length %s",
                    len(grade_profile), len(vle))


    except (Exception, Error) as error:
        logger.error("Error while connecting to PostgreSQL %s", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            logger.info("PostgreSQL connection is closed")

    #loading the data to pd
    grade_profile = pd.DataFrame(grade_profile, columns = ["code_module","code_presentation","id_student","id_assessment","weight","score","still_enrolled"])
    grade_profile["id_assessment"]=pd.to_numeric(grade_profile["id_assessment"])

    vle = pd.DataFrame(vle, columns = ["site","activity_type"])
    return grade_profile, vle