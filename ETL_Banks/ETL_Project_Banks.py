# Code for ETL operations on Country-GDP data
"""
Scenario:

You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be
used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD.
Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate
information that has been made available to you as a CSV file. The processed information table is to be saved locally in
a CSV format and as a database table.

Your job is to create an automated system to generate this information so that the same can be executed in every
financial quarter to prepare the report.

"""

# Importing the required libraries
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import requests

# initializing known entities
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_path = ('./Largest_banks_data.csv')

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows[1:]:
        col = row.find_all('td')
        #print(col[2])
        if len(col) > 1:
            link1 = col[1].find_all('a')
            link2 = link1[1]
            col2 = col[2]
            mc_usd_billion = col2.text.strip()

            Bank_Name = link2.get('title')

            data_dict = {"Name": Bank_Name,
                         "MC_USD_Billion": mc_usd_billion}
            #print(data_dict)
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    #read the exchangerate csv:
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    exr_df = pd.read_csv('exchange_rate.csv')
    exchange_rate = exr_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]


    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries Complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

query_statement = 'SELECT * FROM Largest_banks'
run_query(query_statement, sql_connection)

query_statement = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement, sql_connection)

query_statement = 'SELECT Name from Largest_banks LIMIT 5'
run_query(query_statement, sql_connection)

log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection Closed')