import pandas as pd
import pm4py
import graphviz
import logging

#importing the functions for reading or creating the event log
from func.event_log import event_log_import

#importing the profile functions
import func.clustering_profiles as profiles

#creating the tables in SQL
import func.sql_load

#importing the module for reading from sql
import func.ontology_processing as ontology

from func.kmeans import preparing_kmeans, kmeans_apply

def partition_event_log (data):    
    data_cl_0 = data[data.cluster == 0]
    data_cl_0 = data_cl_0["case:concept:name"]

    data_cl_1 = data[data.cluster == 1]
    data_cl_1 = data_cl_1["case:concept:name"]

    data_cl_0 = list(data_cl_0)
    data_cl_1 = list(data_cl_1)

    log_0 = pm4py.filter_event_attribute_values(log, attribute_key="case:concept:name", values = data_cl_0, retain=True, level = "case")
    log_1 = pm4py.filter_event_attribute_values(log, attribute_key="case:concept:name", values = data_cl_1, retain=True, level = "case")
    
    return log_0, log_1


logging.basicConfig()

logger_general = logging.getLogger("run_clustering")
logger_general.setLevel(logging.DEBUG)

#obtaining an event log which can be either generated from scratch or read-in
# log = ev.event_log_generation() # generates a new event log from OULAD data
log, log_pd = event_log_import (data_path = "simple_event_log.xes", pandas = True) # reads the event log
logger_general.info ("The available event log has the length %s",len(log))

query_grades = """

SELECT DISTINCT
	registration.code_module,
	assessments.code_presentation,
	student_assessment.id_student,
	assessments.id_assessment,
	assessments.weight,
	student_assessment.score,
	registration.date_unregistration as still_enrolled
	
FROM student_assessment

JOIN assessments ON student_assessment.id_assessment = assessments.id_assessment

LEFT JOIN registration ON student_assessment.id_student = registration.id_student

WHERE assessments.code_presentation = '2014J' OR assessments.code_presentation = '2014B'

"""

query_vle = """

SELECT id_site, activity_type
FROM vle
WHERE vle.code_presentation = '2014J' OR vle.code_presentation = '2014B'

"""

perf_profile = profiles.profile_performance(log_pd)
activity_profile = profiles.profile_activity(log_pd)

#obtaining the domain knowledge information
grade_profile, vle = ontology.domain_knowledge_clustering(query_grades, query_vle)
logger_general.info ("The domain knowledge was obtained: \n %s \n %s", grade_profile.head(3), vle.head(3))
grade_profile = profiles.profile_grade(grade_profile)
vle_profile = profiles.profile_vle(vle, log_pd)

data, X = preparing_kmeans(activity_profile = activity_profile, 
                                performance_profile = perf_profile, 
                                grade_profile = grade_profile,
                                vle_profile = vle_profile)

data = kmeans_apply (data=data, X=X, clusters = 2)

log_0, log_1 = partition_event_log(data)
print(log_0, log_1)
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.visualization.heuristics_net import visualizer as hn_visualizer

heu_net = heuristics_miner.apply_heu(log_0, parameters={heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: 0.8})
gviz = hn_visualizer.apply(heu_net)
hn_visualizer.view(gviz)
hn_visualizer.save(gviz, "figures/hn_clustering_0.png")

heu_net = heuristics_miner.apply_heu(log_1, parameters={heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: 0.8})
gviz = hn_visualizer.apply(heu_net)
hn_visualizer.view(gviz)
hn_visualizer.save(gviz, "figures/hn_clustering_1.png")

