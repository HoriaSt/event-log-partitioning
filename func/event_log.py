import pandas as pd
import pm4py
import graphviz
import datetime

#function for defining the case_id in all tables
def case_id (row, info):
    id= ''
    for i in info:
        id = id +'_'+ str(row[i])
    return id[1:]

#function for converting the unix time to normal date
def time_conversion (date, date_start):
    year_start = int(date_start[0:4])
    
    if date_start[4] == 'B':
        month_start = 2
    else: month_start = 10
    
    x = datetime.datetime(year_start, month_start, 1)
    
    ts = pd.Timestamp(x)
    
    do = pd.tseries.offsets.DateOffset(n = int(date))
    
    return ts+do

print("working")

#StudentAssessment
#case ID is being created by concatenating the below columns
keys = ["id_student", "code_module", "code_presentation"]

data1 = pd.read_csv('../data/'+"studentAssessment.csv")
data2 = pd.read_csv('../data/'+"assessments.csv")

#we only want 2014 data
data = pd.merge(data1, data2, on = 'id_assessment', how = 'inner').query(" code_presentation == '2014J' ")
data.reset_index(inplace=True,drop=True)
dummy = []
time_dummy = []
for i,rows in data.iterrows():
    #creating the case_id
    dummy.append ( case_id(rows, keys) )
    time_dummy.append ( time_conversion(rows["date_submitted"], rows["code_presentation"]) )

el_sa = pd.DataFrame({'case:concept:name': dummy, 'time:timestamp': time_dummy})

el_sa["concept:name"] = "submitAssessment"
el_sa["assessment"] = data["id_assessment"]


##Student VLE
print("VLE")

data = pd.read_csv('../data/'+"studentVle.csv")
data = data.query(" code_presentation == '2014J' ")
data.reset_index(inplace=True,drop=True)

dummy = []
time_dummy = []
for i,rows in data.iterrows():
    #creating the case_id
    dummy.append ( case_id(rows, keys) )
    time_dummy.append ( time_conversion(rows["date"], rows["code_presentation"]) )

el_vle = pd.DataFrame({'case:concept:name': dummy, 'time:timestamp': time_dummy})

el_vle["concept:name"] = "interact"
el_vle["site"] = data["id_site"]
print("VLE Done")


###Student Registration
data = pd.read_csv('../data/'+"studentRegistration.csv")
data = data.query(" code_presentation == '2014J' ")
data.reset_index(inplace=True,drop=True)

#dropping rows where the student did not register -> no registration=no event
data = data.dropna(axis=0, subset = ['date_registration'])

dummy = []
time_dummy = []
for i,rows in data.iterrows():
    #creating the case_id
    dummy.append ( case_id(rows, keys) )
    time_dummy.append ( time_conversion(rows["date_registration"], rows["code_presentation"]) )

el_reg = pd.DataFrame({'case:concept:name': dummy, 'time:timestamp': time_dummy})

el_reg["concept:name"] = "register"

###Students Unregistered
data = pd.read_csv('../data/'+"studentRegistration.csv")
data = data.query(" code_presentation == '2014J' ")
data.reset_index(inplace=True,drop=True)

#dropping rows where the student did not unregister -> no action=no event
data = data.dropna(axis=0, subset = ['date_unregistration'])

dummy = []
time_dummy = []
for i,rows in data.iterrows():
    #creating the case_id
    dummy.append ( case_id(rows, keys) )
    time_dummy.append ( time_conversion(rows["date_unregistration"], rows["code_presentation"]) )

el_unreg = pd.DataFrame({'case:concept:name': dummy, 'time:timestamp': time_dummy})

el_unreg["concept:name"] = "dropped"

###Creating the XES file
event_log = pd.concat([el_sa, el_reg, el_unreg, el_vle], ignore_index = True)

event_log = pm4py.format_dataframe (event_log,
                                   case_id='case:concept:name',
                                   activity_key='concept:name',
                                   timestamp_key='time:timestamp',
                                   timest_format='%Y-%m-%d')

event_log = event_log.sort_values("time:timestamp").reset_index(drop = True)

event_log.drop("@@index",axis=1,inplace=True)


from pm4py.objects.conversion.log import converter as log_converter

parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.DEEP_COPY: True}

event_log = log_converter.apply(event_log, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

print("Converting")
### Downloading to xes file
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
xes_exporter.apply(event_log, '../simple_event_log.xes')