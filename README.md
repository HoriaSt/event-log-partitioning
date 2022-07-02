# Domain Aware Event Log Partitioning

This project represents the code support for my Bachelor's Thesis. It is an algorithm that provides a method for a domain-aware partitioning of an event log. There are two approaches: an automated one using k-means clustering and a semi-automatic filtering method. The folder structure is as followed:
- one *run-partitioning.py* file which runs the semi automatic filtering method;
- one *run-clustering.py* file which runs the automatic clustering method;
- one func folder which includes the necessary functions for the two methods.

## Required Input
The requiered Input for running the programmable code is the OULAD open dataset which can be downloaded from https://analyse.kmi.open.ac.uk/open_dataset

It has to be stored in a folder named "data" on the main folder (or the data path requires adjustment). The code will recreate some csv file before sending the data to PostgreSQL, therefore aditional storage space is requiered

## System Requirements
For the required data, as well as for parsing the cleaned information, aproximatelly 500MBs of storage space are needed.

The requiered packages to be installed are:
- pandas
- psycopg2
- pm4py

Moreover, the user only requires a machine that can run Python and PostgreSQL. The minimum requierements for the two are
- a 1 GHz processor
- 2 GB of RAM (recommended are 4 GB)
- 512 MB of HDD storage space

The code was prepared and run on a Windows 10 machine with a 1.80 Ghz i7-8550U CPU, 8GB of installed RAM memory and sufficient HDD storage space.
