# Domain Aware Event Log Partitioning

This project represents the code support for my Bachelor's Thesis. It is an algorithm that provides a method for a domain-aware partitioning of an event log. There are two approaches: an automated one using k-means clustering and a semi-automatic filtering method. The folder structure is as follows:
- one *run-partitioning.py* file, which runs the semi-automatic filtering method;
- one *run-clustering.py* file, which runs the automatic clustering method;
- one "func" folder, which includes the necessary functions for the two methods.

## Required Input
The required Input for running the programmable code is the OULAD open dataset which can be downloaded from https://analyse.kmi.open.ac.uk/open_dataset

It should be stored in a folder named "data" on the main folder (or the hardcoded data path variable requires adjustment). The code will recreate some CSV files before sending the data to PostgreSQL; therefore, additional storage space is required.

## System Requirements
Approximately 500MBs of storage space is needed for the required data and for parsing the cleaned information. Moreover, creating the event log and parsing it to a XES file requieres an aditional 778MBs of storage.

The required packages to be installed are:
- pandas
- psycopg2
- pm4py

Moreover, the user only requires a machine that can run Python and PostgreSQL. The minimum requirements for the two are
- a 1 GHz processor
- 2 GB of RAM (recommended are 4 GB)
- 512 MB of HDD storage space (for the instalation, in addition to the 1.28 GBs requiered for the data)

The code was prepared and run on a Windows 10 machine with a 1.80 GHz i7-8550U CPU, 8GB of installed RAM and sufficient HDD storage space.

## Future Steps
The code needs to be better prepared in order to closer follow the Google Python Style Guide - https://google.github.io/styleguide/pyguide.html For example, the names of the variables could be improved.
Testing should also be implemented (for e.g. using pytest)

Moreover, better standardization could be achieved by testing the functions on a different dataset. Another useful next step is to convert the PM4Py integration in a ProM pulg-in for easier reusability. 

In terms of improving the functionality, the clustering method could be improved by integrating and comparing other algorithms than K-Means. Another area of improvement could be expending the data used for clustering with student information such as age, region, number of attemps at passing a course etc..
