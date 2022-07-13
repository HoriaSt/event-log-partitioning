import collections
import logging

import pandas as pd
# importing the case generation function
from func.event_log import case_id

# pd settings
pd.options.mode.chained_assignment = None


def profile_activity(log_pd=pd.DataFrame):
    """This function creates the activities profiles used in
    clustering.

    The activity profile is created by counting the number
    of times an activity has been inside a case. It follows
    the methodology developed in "Trace Clustering in
    Process Mining" by Song et all

    Args:
        log_pd: an event log in a pandas dataframe

    Returns: A pandas dataframe with the activity profile

    """

    logger = logging.getLogger("profile_activity")
    logger.setLevel(logging.DEBUG)

    logger.info("Activity Profile is getting created")

    activity_profile = log_pd.groupby("case:concept:name")["concept:name"].apply(list)
    activity_profile = activity_profile.reset_index()

    dummy = list(
        activity_profile["concept:name"].apply(lambda x: collections.Counter(x))
    )
    dummy = pd.DataFrame(dummy)
    dummy["case:concept:name"] = activity_profile["case:concept:name"]
    activity_profile = dummy

    activity_profile = activity_profile.fillna(0)

    logger.debug("Length of activity profile is %s", len(activity_profile))
    logger.debug("Mean of activities is %s", activity_profile.mean(axis=0))

    logger.info("Activity Profile is created")

    return activity_profile


def profile_performance(log_pd=pd.DataFrame):
    """This function creates the performance profiles used in
    clustering.

    The performance profile is created by looking at the time
    it takes for a case to be completed. It also stores the
    number of events needed before completion. It follows
    the methodology developed in "Trace Clustering in
    Process Mining" by Song et all

    Args: log_pd: an event log in a pandas dataframe

    Returns: A pandas dataframe with the performance profile

    """
    logger = logging.getLogger("performance_profile")
    logger.setLevel(logging.DEBUG)

    logger.info("Performance Profile is getting created")

    perf_profile = log_pd.groupby("case:concept:name")["time:timestamp"].apply(list)
    perf_profile = perf_profile.reset_index()

    # total case duration
    perf_profile["duration"] = perf_profile["time:timestamp"].apply(
        lambda x: (x[-1] - x[0]).days
    )
    # number of events
    perf_profile["events"] = perf_profile["time:timestamp"].apply(lambda x: len(x))

    # dropping the dummy column
    perf_profile.drop("time:timestamp", axis=1, inplace=True)

    logger.debug("Length of activity profile is %s", len(perf_profile))
    logger.debug("Mean of duration is %s", perf_profile.mean(axis=0))

    logger.info("Performance Profile is created")

    return perf_profile


def profile_grade(grade_profile=pd.DataFrame):
    """This function creates the grades profile used in
    clustering.

    The grade profile is creating the average grade of
    a student (case) present in the event log

    Args:
        grade_profile:
            a pd dataframe containing domain knowledge
            information about grades

    Returns: A pandas dataframe with the grade profile

    """
    keys = ["id_student", "code_module", "code_presentation"]

    logger = logging.getLogger("grade_profile")
    logger.setLevel(logging.DEBUG)

    logger.info("Creating the grades profile started")
    # mimicking the case_id creation from the event log
    dummy = []
    for i, rows in grade_profile.iterrows():
        dummy.append(case_id(rows, keys))

    grade_profile["case:concept:name"] = dummy

    grade_profile["score"] = grade_profile["score"] * grade_profile["weight"] / 100

    grade_profile = grade_profile[["case:concept:name", "score"]]
    grade_profile.reset_index(drop=True, inplace=True)

    grade_profile["count"] = grade_profile["case:concept:name"].copy()

    grade_profile = grade_profile.groupby(["case:concept:name"]).agg(
        {"score": "sum", "count": "count"}
    )
    logger.debug("Length of the grades profile is \n %s", len(grade_profile))
    logger.debug("Mean grades are: \n %s", grade_profile.mean(axis=0))

    grade_profile.reset_index(inplace=True)
    grade_profile.drop(["count"], axis=1, inplace=True)

    logger.info("Grades Profile created")
    return grade_profile


def profile_vle(vle=pd.DataFrame, log_pd=pd.DataFrame):
    """This function creates the vle profile used in
    clustering.

    The grade vle is obtained by checking how many times
    a learning page was opened by each trace (as part of
    the interaction activity in the event log)

    Args:
        vle:
            a pd dataframe containing the key between the
            site attribute of the event log and the
            actual associated pages
        log_pd:
            a dataframe which contains the event log

    Returns: A pandas dataframe with the vle profile

    """
    logger = logging.getLogger("vle_profile")
    logger.setLevel(logging.DEBUG)

    vle_profile = log_pd[["case:concept:name", "site"]].merge(vle, on="site")
    logger.info("Creating the vle profile started")
    vle_profile = vle_profile.groupby("case:concept:name")["activity_type"].apply(list)

    vle_profile = vle_profile.reset_index()

    dummy = list(vle_profile["activity_type"].apply(lambda x: collections.Counter(x)))
    dummy = pd.DataFrame(dummy)
    dummy["case:concept:name"] = vle_profile["case:concept:name"]
    dummy = dummy.fillna(0)
    vle_profile = dummy

    logger.debug("Length of the vle profile is \n %s", len(vle_profile))
    logger.debug("Mean vle interactions are: \n %s", vle_profile.mean(axis=0))

    logger.info("vle Profile created")
    return vle_profile
