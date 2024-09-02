# PersonsDataPipeline
Extracting data from the Persons data API 
The aim of this python project is to build a data pipeline ingesting data from the Persons data API and loading it into a SQLite db. 
This project has been divided into several different scripts dealing with a different aspect of the ETL process. 
The aim is to produce a modular, reusuable, readable, easily integrated and testable code. 
3 classes are produced in that regards and called within the main script: 

DataFetcher - to extract the data 

We initialize the DataFetcher with a base URL and a retry policy.
We then fetch persons data based on quantity, gender, and birthday_start. 
Afterwards we implement a DQ check. 
A logic has been defined to ensure each user is dealing with the same dataset fetched from the API by caching the data. 
It is saved within the dir in cached_data.json

DataTransform - to transform the data extracted

First we anonymize sensitive user data while keeping all columns and then we convert birthdates into a 10-year age group. 
Finally we extracting the domain from the email addresses.


LoadView - to load the transformed data into a db and execute queries

We load the transformed df into a SQLite db and perform the queries needed to answer the questions from Objective 2 

With a bit more time to accomplish this project, unit tests could have been designed to test each classes and the main file. 

This python project has as well been dockerized in order to be consistent across different environments.

In order to keep the data consistent for each user, the following logic has been implemented: 

The first time it runs, the API is called 10 times fetching data for 1000 users for each call until it reaches 10000 users as required. The data is then transformed, loaded into a db and then dumped into a cached_data.json file once the main script runs.
The next time it runs the script looks for a cached_data.json file, if it is found it is used if not the API is called 10 times to recreate the dataset needed.


The following commands are running the docker container: 

docker build -t my-python-app .

docker run -it --rm -v /Users/Tibo85/Documents/taxfix:/app/data my-python-app

I'm letting my own dir path but please ensure to update it within config.py, Dockerfile and within the above docker run command. 
