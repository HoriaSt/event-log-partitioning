import pandas as pd
import datetime
import numpy as np
import logging

logging.basicConfig()
logger_general = logging.getLogger("Data_Preprocessing")
logger_general.setLevel(logging.INFO)

def time_conversion (date, date_start):
    '''
        Converts the time as measured by the data source to 
        convetional datetime format. The function also checks
        if the data was already transformed to datetime in 
        order to avoid an error

        Args: 
            date: 
                represents the number of days since the 
                start of the course until the event
            date_start: 
                represents the start date of the course

        Returns: 
            The date of the event in datetime format
    '''

    year_start = int(date_start[0:4])
    
    if date_start[4] == 'B':
        month_start = 2
    else: month_start = 10
    
    if '-2014' in str(date):
        return date

    x = datetime.datetime(year_start, month_start, 1)

    ts = pd.Timestamp(x)

    do = pd.tseries.offsets.DateOffset(n = int(date))
    
    return ts+do

def preproc (data_path, 
             file,
             output, 
             registration = False, 
             time_column = None,
             columns_drop = None):

    ''' This function prepares the data for sending to PostgreSQL

    The function reads in the data and afterwards performs general
    preprocessing for each of the datasets, as well as individual
    preprocessing.

    Args:
        data_path: 
            path to data folder
        file:
            file name to be preprocessed
        output:
            file name where the result gets saved
        registration:
            Boolean value that tells the function if the 
            "studentRegistration" table is being preprocessed or
            not. This table requires additional attention for 
            preparing it right
        time_column:
            A string denoting the name of the column where a date
            needs converting to datetime format. If a date does not
            exist, to be inserted None
        columns_drop: 
            A list with columns that need to be dropped
    
    Return:
        It does not return anything, but it sends the output
        to a csv file.
    
    '''

    data = pd.read_csv(data_path+file)
    logger_general.debug("File %s was successfullt read",file)

    if columns_drop:
        data.drop(columns_drop, axis = 1, inplace=True, errors = "ignore")

    if registration is True:
        
        data.dropna(subset = ["code_module", "code_presentation", "id_student", "date_registration"],inplace=True)
        logger_general.debug("A registration is being processed for file %s",file)
        data["date_unregistration"] = np.isnan(data["date_unregistration"])

    else : data.dropna(inplace=True)

    if time_column:
        logger_general.debug("Time column is being processed for file %s", file)
        time_dummy = []
        for i,rows in data.iterrows():
            #creating the case_id
            time_dummy.append ( time_conversion(rows[time_column], rows["code_presentation"]) )

        data[time_column] = time_dummy

        data[time_column] = pd.to_datetime(data[time_column])
        logger_general.debug("Timestamps successfully converted for file %s", file)

    logger_general.debug("Data is parsed to csv in file %s", output)
    data.to_csv(data_path + output, sep = ',', index = False)

    return

data_path = "data/"

#running the function for each dataset that needs preprocessing
preproc(
    data_path = data_path,
    file="assessments.csv",
    time_column = "date",
    output = "assessments_clean.csv"
)
logger_general.info("%s has been preprocessed and saved in %s", "assessments.csv","assessments_clean.csv")


preproc(    
    data_path = data_path,
    file="vle.csv",
    columns_drop=["week_from","week_to"],
    time_column = None,
    output = "vle_clean.csv"
)
logger_general.info("%s has been preprocessed and saved in %s", "vle.csv","vle_clean.csv")


preproc(
    data_path = data_path,
    file="studentAssessment.csv",
    columns_drop=["date_submitted"],
    time_column = None,
    output = "studentAssessment_clean.csv"
)
logger_general.info("%s has been preprocessed and saved in %s", "studentAssessment.csv","studentAssessment_clean.csv")

preproc(
    data_path = data_path,
    file="studentRegistration.csv",
    time_column = "date_registration",
    registration=True,
    output = "studentRegistration_clean.csv"
)
logger_general.info("%s has been preprocessed and saved in %s", "studentRegistration.csv","studentRegistration_clean.csv")
