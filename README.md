# Query Optimizer Analysis

For my final project in CIS-6500 I have ran and analyzed three separate workloads on PostgreSQL 16. The datasets I used for these tests were the Job, Stack and TPC-H datasets.


# Code layout

queryRunner.py: This file contains the functions for running queries in postgres and parsing their JSON outputs into csv format.

graphs.py: This file contains the code for creating the graphs I used for my analysis.[

cleaningFunctions.py: This file was used to clean some of the original data files.

# Hardware
All queries were ran on an Apple M2 Mac mini with 8 GB of ram.
