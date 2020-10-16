from datetime import datetime

import pandas as pd

import json

from pymongo import MongoClient
from pprint import pprint
from pymongo.errors import OperationFailure

import time
import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import pandas as pd
import csv
import os
from datetime import date

import numpy as np


def main():
    URL = "https://projects.fivethirtyeight.com/polls/president-general/"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    tables = soup.find_all("table")
    df_son = func(tables)
    print(df_son.head())
    insert_mongo(df_son)
  
    
def insert_mongo(df_son):
        USERNAME = 'hmtkvs'
        PASSWORD = 'Asdasd94-35'
        client = MongoClient("mongodb+srv://{0}:{1}@clusternews.prjdz.mongodb.net/test?retryWrites=true&w=majority".format(USERNAME, PASSWORD)) 
        
        db = client.get_database('bias')
        records = db['Polls']
        
        try:
            client.list_database_names()
            print('Data Base Connection Established........')
        except OperationFailure as err:
            print(f"Data Base Connection failed. Error: {err}")
            
        count_before = records.count_documents({})
        print("Number of Records before insert:", count_before)
      
        poll_dict = df_son.to_dict(orient='records')   
        
        try:
            records.insert_many(poll_dict)
        except (OperationFailure, TypeError) as e:
            print(e)
            pass
            
        count_after = records.count_documents({})
        print("Final Number of Records:", count_after)  
        print("DONE!")

def update_date(str_):
    #print(str_)
    try:
        str_ = str_.split(',')[0]
        str_0 = str_.split('-')[0]
        str_1 = str_.split('-')[1]
        
        if str_1.isnumeric():
             str_1 = str_.split(' ')[0] + ' '  +  str_1 
             
        str_tuple = (str_0, str_1)
        return str_tuple
    except:
        str_ = str_.split(',')[0]
        str_0 = str_.split('-')[0]
        
        return str_0
    
def split_int(test_str):
    import re 
    temp = re.compile("([a-zA-Z]+)([0-9]+)") 
    res = re.findall(r'(\w+?)(\d+)', test_str)
    return res
    
def remove_space(test_str):
    return test_str.replace(u'\xa0', u' ')
    
def pollaster_score(test_str):
    return test_str[0]
    
def func(tables):
    tab_data = []
    for table in tables:
        tab_data.extend([[cell.text for cell in row.find_all(["th","td"])]
                                for row in table.find_all("tr")])
    df = pd.DataFrame(tab_data)
    #df = df.iloc[:,[0,4,5,8,9,10,11,12]]
    #df.columns = ['Type', 'Date', 'Pollester', 'Name1', 'Perc1', 'Mix', 'Perc2', 'Name2']
    df = df.iloc[:,[0,4,5,10]]
    df.columns = ['Type', 'Date', 'Pollester', 'Result']

    df = df[df['Result'].notna()]
    df['Date'] = df['Date'].apply(remove_space)
    df['Date'] = df['Date'].apply(update_date)
    df['Result'] = df['Result'].apply(split_int)
    df['Pollester_Score'] = df['Pollester'].apply(pollaster_score)
    
    return df
    
if __name__ == "__main__":
    main()