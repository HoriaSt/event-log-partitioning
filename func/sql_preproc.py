import pandas as pd
import datetime
import numpy as np

def time_conversion (date, date_start):

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


## assessments preproc
assessments = pd.read_csv("../data/assessments.csv")

assessments.dropna(inplace=True)

time_dummy = []
for i,rows in assessments.iterrows():
    #creating the case_id
    time_dummy.append ( time_conversion(rows["date"], rows["code_presentation"]) )

assessments.date = time_dummy

assessments["date"] = pd.to_datetime(assessments["date"] )

assessments.to_csv("../data/assessments_clean.csv",sep=',',index=False)


## vle preproc
vle = pd.read_csv("../data/vle.csv")
vle.drop(["week_from","week_to"], axis = 1, inplace=True, errors = "ignore")


vle.dropna(inplace=True)
vle.to_csv("../data/vle_clean.csv",sep=',',index=False)

## student assessment preproc
student_assessments = pd.read_csv("../data/studentAssessment.csv")

student_assessments.dropna(inplace=True)

student_assessments.drop(["date_submitted"], axis = 1, inplace=True, errors = "ignore")

student_assessments.to_csv("../data/studentAssessment_clean.csv",sep=',',index=False)

### student registration data
registration = pd.read_csv("../data/studentRegistration.csv")

registration.dropna(subset = ["code_module", "code_presentation", "id_student", "date_registration"],inplace=True)


time_dummy_reg = []
for i,rows in registration.iterrows():
    #creating the timestamp
    time_dummy_reg.append ( time_conversion(rows["date_registration"], rows["code_presentation"]) )

registration.date_registration = time_dummy_reg
registration.date_unregistration = np.isnan(registration["date_unregistration"])

registration.to_csv("../data/studentRegistration_clean.csv",sep=',',index=False)

print("data preprocesssed")

