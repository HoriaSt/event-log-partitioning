import pandas as pd
import pm4py
import collections
import graphviz
import logging

def profile_activity(log_pd = pd.DataFrame):
    ''' This function creates the activities profiles used in 
        clustering.

        The activity profile is created by counting the number
        of times an activity has been inside a case. It follows
        the methodology developed in "Trace Clustering in 
        Process Mining" by Song et all

        Args:
            log_pd: an event log in a pandas dataframe

        Returns: A pandas dataframe with the activity profile
    
    '''

    logger = logging.getLogger("profile_activity")
    logger.setLevel(logging.DEBUG)

    logger.info("Activity Profile is getting created")

    activity_profile = log_pd.groupby("case:concept:name")["concept:name"].apply(list)
    activity_profile = activity_profile.reset_index()
    
    logger.debug("Length of activity profile is %s", len(activity_profile))

    dummy = list(activity_profile["concept:name"].apply(lambda x : collections.Counter(x)))
    dummy = pd.DataFrame(dummy)
    dummy["case:concept:name"] = activity_profile["case:concept:name"]
    activity_profile = dummy

    activity_profile = activity_profile.fillna(0)    
    
    logger.debug("Mean of activities is %s", activity_profile.mean(axis=0))

    logger.info("Activity Profile is created")

    return activity_profile

def profile_performance (log_pd = pd.DataFrame):
    ''' This function creates the performance profiles used in 
        clustering.

        The per performance is created by looking at the time
        it takes for a case to be completed. It follows
        the methodology developed in "Trace Clustering in 
        Process Mining" by Song et all

        Args:
            log_pd: an event log in a pandas dataframe

        Returns: A pandas dataframe with the activity profile
    
    '''

    performance_profile = log_pd.groupby("case:concept:name")["time:timestamp"].apply(list)
    performance_profile = performance_profile.reset_index()

    #total case duration
    performance_profile["duration"] = performance_profile["time:timestamp"].apply(lambda x: ( x[-1]-x[0] ).days )
    #number of events
    performance_profile["events"] = performance_profile["time:timestamp"].apply(lambda x: len(x))

    #dropping the dummy column
    performance_profile.drop("time:timestamp",axis=1,inplace = True)

    return performance_profile


