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
    
extract_raw_job_data(url, headers, querystring)