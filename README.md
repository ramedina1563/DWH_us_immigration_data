# DWH_us_immigration_data
Scope
This projects examines the immigration patterns in the US. For this, 4 datasets are used that contain information not only of single persons entering the country, but also of the places they arrive at and the characteristics of these ports of entry. The data used is contained in an attached disk in Udacity. This data will be loaded by using pandas in order to analyze it and clean it to guarantee a good quality of it. Afterwards, these data will be saved to S3 buckets. The data contained in the S3 buckets will be copied into Redshift as staging tables, where they will be further processed until a star schema is obtained. Airflow will be used starting with the loading step to Redshift until the data quality checks are performed after the structuring of the star schema has succeded.

Datasets used in the project
Following datasets were used for this project:

I94 Immigration Data: This data lists all applications made to enter to the USA as an immigrant during 2016.
World Temperature Data: This dataset contains relevant information about climate at several cities around the world. The dataset contains the average temperature of cities and countries monthly calculated for a long period of time. The data comes from Kaggle
U.S. City Demographic Data: This dataset provides mainly demographic statistics of cities in the US. The information contained in this dataset include city, state, median age, population, race, among others. The data comes from OpenSoft
Airport Code Table: As the name says, this dataset provides all information related to airports around the world. The information contained here includes: type of airport, latitude, longitude, airport's city, among others. The data comes from datahun.io

You'll find following files in the project:
1. Capstone Project Template.ipynb: This Notebook contains all steps in charge of doing the data preparation as well as a description of the steps to take on.
2. dags/capstone.py: This code creates the main dag for the analysis on Airflow
3. dags/create_tables.py: This code contains the functions that create the tables defined in the defined schema. This file is required by capstone.py 
4. plugins/__init__.py: Loads helpers and operators in folder plugins
5. plugins/helpers/sql_queries.py: Code containing INSERT and COPY command. This file is required by capstone.py
6. plugins/helpers/__init__.py
7. plugins/operators/__init__.py
8. plugins/operators/data_quality.py: Function in charge of performing data quality checks. This file is required by capstone.py
9. plugins/operators/load_dimension.py: Function in charge of loading dimension table to Redshift. This file is required by capstone.py
10. plugins/operators/load_fact.py: Function in charge of loading fact table to Redshift. This file is required by capstone.py
11. plugins/operators/s3_to_redshift.py: Function in charge of copyingg the data from S3 to Redshifft. This file is required by capstone.py
12. plugins/operators/stage_redshift.py: Function in charge of creating staging table in Redshift. This file is required by capstone.py
