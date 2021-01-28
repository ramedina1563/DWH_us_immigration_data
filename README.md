# DWH_us_immigration_data
Scope
This projects examines the immigration patterns in the US. For this, 4 datasets are used that contain information not only of single persons entering the country, but also of the places they arrive at and the characteristics of these ports of entry. The data used is contained in an attached disk in Udacity. This data will be loaded by using pandas in order to analyze it and clean it to guarantee a good quality of it. Afterwards, these data will be saved to S3 buckets. The data contained in the S3 buckets will be copied into Redshift as staging tables, where they will be further processed until a star schema is obtained. Airflow will be used starting with the loading step to Redshift until the data quality checks are performed after the structuring of the star schema has succeded.

Datasets used in the project
Following datasets were used for this project:

I94 Immigration Data: This data lists all applications made to enter to the USA as an immigrant during 2016.
World Temperature Data: This dataset contains relevant information about climate at several cities around the world. The dataset contains the average temperature of cities and countries monthly calculated for a long period of time. The data comes from Kaggle
U.S. City Demographic Data: This dataset provides mainly demographic statistics of cities in the US. The information contained in this dataset include city, state, median age, population, race, among others. The data comes from OpenSoft
Airport Code Table: As the name says, this dataset provides all information related to airports around the world. The information contained here includes: type of airport, latitude, longitude, airport's city, among others. The data comes from datahun.io
