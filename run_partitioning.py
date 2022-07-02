#python packages that are imported
import pandas as pd
import pm4py
import psycopg2
import logging
import graphviz

pd.options.mode.chained_assignment = None


#loading the module for event log generation and reading in
from func.event_log import event_log_import, event_log_generation

#loading the module for preprocessing and extracting the domain knowledge
import func.ontology_processing as ontology

#creating the ontology in sql
import func.sql_load


query = """

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


def domain_selected_cases (knowledge, domain_rule=str):
    ''' Function that applies a expert defined function for 
        splitting cases in wanted and unwanted traces

        Args:
            knowledge: 
                a pandas dataframe that lists all cases to
                be selected from
            domain_rule: 
                expert defined filtering statement which
                is used to select wanted cases

        Returns:
            A list of case ids which are considered wanted

    '''
    passed_course = knowledge["case:concept:name"].where( eval(domain_rule) ).dropna()
    passed_course = list(passed_course)

    return passed_course


def partition_event_log (knowledge, log):
    ''' Function that partitions the event log

        Args: 
            knowledge: 
                list of wanted cases
            log:
                event log to be partitioned
        Returns:
            Two event logs, one with wanted traces
            and another with unwanted traces
    '''
    from pm4py import filter_event_attribute_values as filter_events

    log_positive = filter_events(log, 
                                attribute_key="case:concept:name", 
                                values = knowledge, 
                                retain=True, 
                                level = "case")

    log_negative = filter_events(log, 
                                attribute_key="case:concept:name", 
                                values = knowledge, 
                                retain=False, 
                                level = "case")
    
    return log_positive, log_negative


#creating a logger for the run file
logging.basicConfig()

logger_general = logging.getLogger("run_partioning")
logger_general.setLevel(logging.DEBUG)


#obtaining an event log which can be either generated from scratch or read-in

# log = ev.event_log_generation() # generates a new event log
log = event_log_import (data_path = "simple_event_log.xes") # reads the event log
logger_general.info ("The available event log has the length %s",len(log))

#converting the event log to pandas
import pm4py.objects.conversion.log.converter as converter
log_pd = converter.apply(log, variant = converter.Variants.TO_DATA_FRAME)
logger_general.info ("The event log was converted to pandas")

#obtaining the domain knowledge information
knowledge = ontology.domain_knowledge_processing(query=query)
logger_general.info ("The domain knowledge was obtained", knowledge.head(3))


#selecting the cases which we are going to split for
domain_rule = ''' (knowledge["score"] >= 40) & (knowledge["still_enrolled"] == True) '''
knowledge = domain_selected_cases(knowledge = knowledge, domain_rule = domain_rule)
logger_general.info ("The cases for the split were obtained:. An example of first 3 is: %s", knowledge[:3])


#splitting the event log for wanted and unwanted traces
logger_general.info ("Splitting started")
log_wanted, log_unwanted = partition_event_log(knowledge = knowledge, log = log)
logger_general.info ("Splitting ended succesfully. Wanted traces %s. Unwanted traces %s", len(log_wanted),len(log_unwanted))



logger_general.info("Creating vizualizations")
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.visualization.heuristics_net import visualizer as hn_visualizer

heu_net = heuristics_miner.apply_heu(log, parameters={heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: 0.99})
gviz = hn_visualizer.apply(heu_net)
hn_visualizer.view(gviz)
hn_visualizer.save(gviz, "figures/hn_all.png")

heu_net = heuristics_miner.apply_heu(log_wanted, parameters={heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: 0.99})
gviz = hn_visualizer.apply(heu_net)
hn_visualizer.view(gviz)
hn_visualizer.save(gviz, "figures/hn_positive.png")

heu_net = heuristics_miner.apply_heu(log_unwanted, parameters={heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: 0.99})
gviz = hn_visualizer.apply(heu_net)
hn_visualizer.view(gviz)
hn_visualizer.save(gviz, "figures/hn_negative.png")


