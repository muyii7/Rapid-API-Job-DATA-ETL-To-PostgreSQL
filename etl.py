import requests
import json
import pandas as pd
import ast
import psycopg2
from datetime import datetime
import os
from util import get_api_credentials, get_database_conn
from dotenv import dotenv_values
dotenv_values()


#the rapidapi.com url for connecting with the API credentials
url = "https://jsearch.p.rapidapi.com/search" 

#the credentials to for the API are pulled
config = dotenv_values('.env')
headers = get_api_credentials()[0]
querystring = get_api_credentials()[1]
raw_folders = 'raw_folder/'

#data is extracted from rapidapi.com and laoded to local folder called raw_folder
def extract_raw_job_data(url, headers, querystring): 
    config = dotenv_values('.env')
    headers = ast.literal_eval(config.get('HEADERS'))
    querystring = ast.literal_eval(config.get('QUERYSTRING'))
    file_name = f"raw_data_{datetime.now().strftime('%Y%m%d%H%M')}" #file name is defined per time
    response = requests.get(url, headers=headers, params=querystring).json() #data extraction from API
    try:
        with open(f"raw_folder/{file_name}.json","w") as file:
            json.dump(response,file) #extracted data is converted to JSON format
                
    except Exception as e:
        print(f"Error writting JSON data to raw_data folder: {str(e)}")
    print('raw data job is extracted from api and written to raw_folder')
    

#json data is read from the local raw_folder then transformed before being pushed to the postgreSQL database
def job_data_transformation(raw_folder):
    path_to_json = raw_folder
    # Get a list of all JSON files in the folder
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

    # Sort the list of files by modification time (latest first)
    json_files.sort(key=lambda x: os.path.getmtime(os.path.join(path_to_json, x)), reverse=True)

    # Read the contents of the latest JSON file into a pandas DataFrame
    latest_json_file = json_files[0]
    with open(f'raw_folder/{latest_json_file}', 'r') as json_file:
        json_data = json.load(json_file)
    #transformation of the read json file
    try:
        data_job = json_data.get('data')
        columns = ['employer_website','job_id', 'job_employment_type','job_title', 'job_apply_link','job_description','job_city','job_country','job_posted_at_datetime_utc', 'employer_company_type']
        job_data = pd.DataFrame(data_job)[columns]
        job_data['job_posted_date'] = job_data['job_posted_at_datetime_utc'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').date())
        job_data = job_data[['employer_website','job_id', 'job_employment_type','job_title', 'job_apply_link','job_description','job_city','job_country','job_posted_date', 'employer_company_type']]
        
    except Exception as e:
        print(f"Error reading JSON file from raw_data folder: {e}")
    file_name = f"transformed_data_{datetime.now().strftime('%Y%m%d%H%M')}.csv" #file name is defined per time
    job_data.to_csv(f"transformed_data/{file_name}", index=False) #csv data os written to transformed data folder
    print('transformed data is written to transformed folder')
job_data_transformation(raw_folders)


    
