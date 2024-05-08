import pandas as pd
import psycopg2
from datetime import datetime
import ast
from sqlalchemy import create_engine
from dotenv import dotenv_values
dotenv_values()

#the rapidapi.com url for connecting with the API credntials
url = "https://jsearch.p.rapidapi.com/search"

def get_api_credentials():
    config = dotenv_values('.env')
    headers = ast.literal_eval(config.get('HEADERS'))
    querystring = ast.literal_eval(config.get('QUERYSTRING'))
    return headers, querystring


# Get credentials from environment variable file
def get_database_conn(): #used to connect to the DB where the file log is stored
    config = dotenv_values('.env')
    db_user_name = config.get('DB_USER_NAME')
    db_password = config.get('DB_PASSWORD')
    db_name = config.get('DB_NAME')
    port = config.get('PORT')
    host = config.get('HOST')
    conn = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')
    return conn
# print('connection to postgresql is good')
get_database_conn()
