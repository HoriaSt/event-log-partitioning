import datetime
import logging
from distutils.debug import DEBUG
from distutils.log import debug

import pandas as pd
import pm4py


def case_id(row, info):
    """Function that creates the case id for every case based on
    predefined rules.

    Args:
        row:
            the row from the df where the event appears
        info:
            information that has to be included in the id

    Returns:
        The generated case id. An example is: 100064_FFF_2013J
    """

    id = ""

    for i in info:
        id = id + "_" + str(row[i])

    id = id[1:]
    return id


def time_conversion(date, date_start):
    """
    Converts the time as measured by the data source to
    convetional datetime format

    Args:
        date:
            represents the number of days since the
            start of the course until the event
        date_start:
            represents the start date of the course

    Returns:
        The date of the event in datetime format
    """
    year_start = int(date_start[0:4])

    if date_start[4] == "B":
        month_start = 2
    else:
        month_start = 10

    x = datetime.datetime(year_start, month_start, 1)

    ts = pd.Timestamp(x)

    do = pd.tseries.offsets.DateOffset(n=int(date))

    return ts + do


def student_assessment(
    keys=list,
    data_path=str,
    file_assessments="assessments.csv",
    file_students="studentAssessment.csv",
):

    """Reads-in the information about students and their assigments
    and extracts the information needed for the event log.

    Args:
        keys:
            the columns used in the creation of the case id
        data_path:
            The path to the folder with data
        file_assessments:
            Name of the file where the assessments information
            can be found
        file_students:
            Name of the file where each assessment of each
            student can be found

    Returns:
        A pandas dataframe that can be easily converted to
        an event log

    """
    logger = logging.getLogger("student_assessment")
    logger.setLevel(logging.INFO)

    data_S = pd.read_csv(data_path + file_assessments)
    data_a = pd.read_csv(data_path + file_students)
    logger.debug("student assessment data successfully read")

    # we only want 2014 data
    data = pd.merge(data_S, data_a, on="id_assessment", how="inner").query(
        " code_presentation == '2014J' "
    )

    data.reset_index(inplace=True, drop=True)
    logger.debug("student assessment data successfully merged")

    dummy = []
    time_dummy = []
    for i, rows in data.iterrows():
        # creating the case_id
        dummy.append(case_id(rows, keys))
        time_dummy.append(
            time_conversion(rows["date_submitted"], rows["code_presentation"])
        )
    logger.debug("student assessment ids and timestamps successfully generated")

    # creating the result
    el_sa = pd.DataFrame({"case:concept:name": dummy, "time:timestamp": time_dummy})

    # filling in the action type
    el_sa["concept:name"] = "submitAssessment"
    # filling in an attribute related to the assessment
    el_sa["assessment"] = data["id_assessment"]

    logger.debug("event log for student asssessment successfully generated")

    return el_sa


def student_registration(keys=list, data_path=str, file="studentRegistration.csv"):
    """
    Reads in registration data about students and extracts events
    from a csv file

    Args:
        keys:
            the columns used in the creation of the case id
        data_path:
            The path to the folder with data
        file:
            Name of the file where the registration information
            can be found

    Returns:
        A pandas dataframe with registration events that can be
        easily converted to an event log
    """
    logger = logging.getLogger("student_registration")
    logger.setLevel(logging.INFO)

    data = pd.read_csv(data_path + file)
    logger.debug("registration data successfully read")

    # we are interested in only a part of the event log
    data = data.query(" code_presentation == '2014J' ")
    data.reset_index(inplace=True, drop=True)
    logger.debug("registration data succesfully filtered for 2014J")

    # dropping the rows where the student did not register -> no registration=no event
    data = data.dropna(axis=0, subset=["date_registration"])

    dummy = []
    time_dummy = []
    for i, rows in data.iterrows():
        # creating the case_id
        dummy.append(case_id(rows, keys))
        time_dummy.append(
            time_conversion(rows["date_registration"], rows["code_presentation"])
        )

    logger.debug("registration case ids and timestamps succesfully created")

    # creating an event log dataframe
    el_reg = pd.DataFrame({"case:concept:name": dummy, "time:timestamp": time_dummy})
    el_reg["concept:name"] = "register"

    logger.debug("event log for REGISTRATIONS successfully generated")

    return el_reg


def student_unregistration(keys=list, data_path=str, file="studentRegistration.csv"):
    """
    Reads in unregistration data about students and extracts events
    from a csv file

    Args:
        keys:
            the columns used in the creation of the case id
        data_path:
            The path to the folder with data
        file:
            Name of the file where the unregistration information
            can be found

    Returns:
        A pandas dataframe with unregistration events that can be
        easily converted to an event log
    """

    logger = logging.getLogger("student_unregistration")
    logger.setLevel(logging.INFO)

    data = pd.read_csv(data_path + file)
    logger.debug("unregistration data successfully read")

    # we are interested in only a part of the event log
    data = data.query(" code_presentation == '2014J' ")
    data.reset_index(inplace=True, drop=True)
    logger.debug("unregistration data succesfully filtered for 2014J")

    # dropping rows where the student did not unregister -> no action=no event
    data = data.dropna(axis=0, subset=["date_unregistration"])

    dummy = []
    time_dummy = []
    for i, rows in data.iterrows():
        # creating the case_id
        dummy.append(case_id(rows, keys))
        time_dummy.append(
            time_conversion(rows["date_unregistration"], rows["code_presentation"])
        )

    logger.debug("unregistration case ids and timestamps succesfully created")

    # creating an event log dataframe
    el_unreg = pd.DataFrame({"case:concept:name": dummy, "time:timestamp": time_dummy})
    el_unreg["concept:name"] = "dropped"

    logger.debug("event log for UNREGISTRATIONS successfully generated")

    return el_unreg


def vle_interaction(keys=list, data_path=str, file="studentVle.csv"):
    """
    Reads in Virtual Learning Environment (vle) interaction
    data about students and extracts events from a csv file

    Args:
        keys:
            the columns used in the creation of the case id
        data_path:
            The path to the folder with data
        file:
            Name of the file where the vle interaction information
            can be found

    Returns:
        A pandas dataframe with vle interaction events that can be
        easily converted to an event log
    """
    logger = logging.getLogger("vle_interaction")
    logger.setLevel(logging.DEBUG)

    data = pd.read_csv(data_path + file)
    logger.debug("vle data successfully read")

    # we are interested in only a part of the event log
    data = data.query(" code_presentation == '2014J' ")
    data.reset_index(inplace=True, drop=True)
    logger.debug("vle data succesfully filtered for 2014J")

    dummy = []
    time_dummy = []
    for i, rows in data.iterrows():
        # creating the case_id
        dummy.append(case_id(rows, keys))
        time_dummy.append(time_conversion(rows["date"], rows["code_presentation"]))
    logger.debug("vle case ids and timestamps succesfully created")

    # creating an event log dataframe
    el_vle = pd.DataFrame({"case:concept:name": dummy, "time:timestamp": time_dummy})
    el_vle["concept:name"] = "interact"
    el_vle["site"] = data["id_site"]

    logger.debug("event log for VLE successfully generated")

    return el_vle


def event_log_generation():
    """
    This function acts as a main function for event log generation

    It runs all the above functions in order to achieve an event log.
    It handles preprocessing of the data from the relational db and
    it structures the extraction in a pandas dataframe built following
    the rigors of pm4py. Afterwardsm the pd dataframe gets converted
    to an event log, which also gets parsed to a .xes file format for
    persistance. The runtime of the function is significant.

    Returns:
       An event log in the pm4py environment.
    """
    # starting a logger
    logging.basicConfig()
    logger_general = logging.getLogger("general")
    logger_general.setLevel(logging.INFO)

    logger_general.info("The event log generation has STARTED")

    # StudentAssessment
    # case ID will be created from the following keys
    keys = ["id_student", "code_module", "code_presentation"]
    data_path = "data/"

    # applying the previously created fucntions for extracting event logs
    student_assessment = student_assessment(keys=keys, data_path=data_path)
    logger_general.info("Student Assessments events created")

    student_registration = student_registration(keys=keys, data_path=data_path)
    logger_general.info("Student Registration events created")

    student_unregistration = student_unregistration(keys=keys, data_path=data_path)
    logger_general.info("Student Unregistration events created")

    vle_interaction = vle_interaction(keys=keys, data_path=data_path)
    logger_general.info("VLE interaction events created")

    # concatenating the results
    event_log = pd.concat(
        [
            student_assessment,
            student_registration,
            student_unregistration,
            vle_interaction,
        ]
    )

    # formatting the results for process minig
    event_log = pm4py.format_dataframe(
        event_log,
        case_id="case:concept:name",
        activity_key="concept:name",
        timestamp_key="time:timestamp",
        timest_format="%Y-%m-%d",
    )

    event_log = event_log.sort_values("time:timestamp").reset_index(drop=True)
    event_log.drop("@@index", axis=1, inplace=True)
    logger_general.info("Data Concatenated and Formated")

    # converting data to xes format
    from pm4py.objects.conversion.log import converter as log_converter

    parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.DEEP_COPY: True}
    event_log = log_converter.apply(
        event_log, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG
    )
    logger_general.info("Data Converted to xes format")

    # Exporting data to xes file
    from pm4py.objects.log.exporter.xes import exporter as xes_exporter

    xes_exporter.apply(event_log, "simple_event_log.xes")

    return event_log


def event_log_import(data_path=str, pandas=False):
    """This function reads an event log from an .xes file

    Args:
        data_path: the path to the .xes file
        pandas: a boolean value that tells if the event
                log should also be converted to a pandas
                dataframe or not. Prefilled with False

    Returns:
        Returns an event log in the pm4py environemnt. Optionally
        (if pandas == True) it can also return a pandas dataframe

    """
    from pm4py.objects.log.importer.xes import importer as xes_importer

    logger = logging.getLogger("event_log_import")
    logger.setLevel(logging.DEBUG)

    logger.info("Event log is being read from %s", data_path)

    # can be changed to LINE_BY_LINE for better performance
    variant = xes_importer.Variants.ITERPARSE

    parameters = {variant.value.Parameters.TIMESTAMP_SORT: True}
    logger.debug("Parameters for the event log are: %s", parameters)
    log = xes_importer.apply(data_path, variant=variant, parameters=parameters)
    logger.debug("Length of the event log is %s", len(log))
    logger.info("Event log read")

    if pandas is True:

        # converting to pandas if we pandas == True
        import pm4py.objects.conversion.log.converter as converter

        log_pd = converter.apply(log, variant=converter.Variants.TO_DATA_FRAME)

        logger.info("The event log was converted to pandas")

        return log, log_pd

    return log
