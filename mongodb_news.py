from pymongo import MongoClient
from pprint import pprint
from pymongo.errors import OperationFailure

import requests
import pandas as pd

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
from datetime import datetime

import math
import sys
import yaml
import logging


class News_Handler(): 
    def __init__(self):
    
        #Credentials
        with open("credentials.yml", 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        
        self.username = cfg['username']#'hmtkvs'
        self.password = cfg['password']#'Asdasd94-35'
        
        
         
        #Parameters for the NewsAPI
        self.keyword = 'election' + '&'
        self.database = cfg['database']#'bias'
        self.collection = cfg['collection_news']#'NewsData'
        self.api_key = cfg['news_api_key']#'189ee9100e854338aaa9d4e4f992a2cf'
        ##Date
        date_time = datetime.now()
        self.startDate = str(date_time.strftime("%Y-%m-%d")) + '&'
        print(self.startDate)
        
        #Initiate
        self.keys = ["id", "name", "author", "title", "description", "url", "urlToImage", "publishedAt", "content"]
        self.sources = []
        self.news = []
        self.records = ''
        
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        logging.basicConfig(format = '%(asctime)s : %(levelname)s : %(message)s',
                    datefmt = '%m/%d/%Y %I:%M:%S',
                    filename = 'tweets.log',
                    level=logging.INFO)
        
        #Call Functions
        self.connect_mongo()
        self.getSources()
        self.getData()
        
        
        
    def connect_mongo(self):
        USERNAME = self.username
        PASSWORD = self.password
        client = MongoClient("mongodb+srv://{0}:{1}@clusternews.prjdz.mongodb.net/test?retryWrites=true&w=majority".format(USERNAME, PASSWORD)) 
        
        db = client.get_database(self.database)
        self.records = db[self.collection]
        
        try:
            client.list_database_names()
            print('Data Base Connection Established........')

        except OperationFailure as err:
            print(f"Data Base Connection failed. Error: {err}")
            
    def getSources(self):
        url = 'https://newsapi.org/v2/sources?language=en&country=us&apiKey=189ee9100e854338aaa9d4e4f992a2cf'
        response = requests.get(url)
        
        for i in range(0, len(response.json()['sources'])):
            self.sources.append(response.json()['sources'][i]['id'])
            
        print("total news:", len(self.sources))
        
        return self.sources
    
    def extract_values(self, obj, key):
        arr = []
        
        def extract(obj, arr, key):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        extract(v, arr, key)
                    elif k == key:
                        arr.append(v)
            elif isinstance(obj, list):
                for item in obj:
                    extract(item, arr, key)
            return arr

        results = extract(obj, arr, key)
        return results
            
    def getData(self):
        #source = 'the-washington-times&'      
        for i in self.sources:
            source = str(i) + '&'
            print("---------------------------------------------------------------------------------")
            print(source)
            logging.info('Source:'.format(source))
            url = ('http://newsapi.org/v2/everything?'
                   'sources=' + '{0}'
                   'q =' + '{1}'
                   'from=' + '{2}'
                   'apiKey=' + '{3}').format(source, self.keyword, self.startDate, self.api_key)
                   
            response = requests.get(url)           
            try:
                data = json.loads(response.text)
                for i in range(len(self.keys)):
                    self.news.append(self.extract_values(data, self.keys[i]))
                    
            except (ConnectionError, Timeout, TooManyRedirects) as e:
                print(e)  

            self.insert_mongo()
        
    def createDF(self):
        newsObj = pd.DataFrame(self.news) 
        newsObj=newsObj.T
        newsObj.rename(columns = {0:'id', 1:'source', 2:'author', 3:'title', 4:'description', 5:'url', 6:'urlToImage', 7:'publishedAt', 8:'content'}, inplace=True)
        newsObj = newsObj.drop(columns=['id', 'urlToImage'])
        print(newsObj.head(2))
        self.news = []
        
        return newsObj
                
    def insert_mongo(self):
        #db.employees.find().pretty()
        
        count_before = self.records.count_documents({})
        print("Number of Records before insert:", count_before)
        newsObj = self.createDF()
        news_dict = newsObj.to_dict(orient='records')   
        try:
            self.records.insert_many(news_dict)
        except (OperationFailure, TypeError) as e:
            print(e)
            pass
        
        count_after = self.records.count_documents({})
        print("Final Number of Records:", count_after)
        logging.info('Number of news inserted:'.format(count_after-count_before))
        
    
        
        
            
News_Handler()
'''
DB_Handler = DB_Handler()
DB_Handler.getSources()
DB_Handler.getData()
'''


