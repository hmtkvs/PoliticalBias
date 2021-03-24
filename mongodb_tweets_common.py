import numpy as np
import pandas as pd
import tweepy
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream

from pymongo import MongoClient
from pprint import pprint
from pymongo.errors import OperationFailure

import logging
import time
import yaml
import sys

class Tweets_Handler(): 
    def __init__(self):
    
        #Credentials
        with open("credentials.yml", 'r') as ymlfile:
            self.cfg = yaml.safe_load(ymlfile)
            
        self.username = self.cfg['username']
        self.password = self.cfg['password']
        
        self.database = self.cfg['database']
        self.collection = self.cfg['collection_tweets_2']
        #self.api_key = cfg['news_api_key']
        
        self.api = ''
        
        self.alltweets = []
        
        self.records = ''
        
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        logging.basicConfig(format = '%(asctime)s : %(levelname)s : %(message)s',
                    datefmt = '%m/%d/%Y %I:%M:%S',
                    filename = 'tweets_common.log',
                    level=logging.INFO)
        
        self.connect_mongo()
        self.getTweets()
        
        
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
            
    def createDF(self, screen_name):
        #transform the tweepy tweets into a 2D array that will populate the csv 
        outtweets = [[tweet.user.screen_name, tweet.id_str, tweet.created_at, tweet.text, tweet.text.split()[-1]] for tweet in self.alltweets 
                      if tweet.text.split()[-1][0:4] == 'http' and tweet.text.split()[0][0:4] != 'http']
        
        #put tweets into DF
        tweets_df = pd.DataFrame(outtweets, columns=['user', 'ID', 'created_at', 'text', 'url'])
        tweets_df['user'] = tweets_df['user'].str.lower()
        
        df_detail = pd.read_csv("Users_common.csv", sep = ";", index_col = False)
        
        pos_list = []
        party_list = [] 
        
        #print("REKLAM",tweets_df[tweets_df['user'] == screen_name])
        
        pos = df_detail['Position'][df_detail['Username'] == screen_name].values[0]
        [pos_list.append(pos) for _ in range(0,len(tweets_df))]
        
        party = df_detail['Party'][df_detail['Username'] == screen_name].values[0]
        [party_list.append(party) for _ in range(0,len(tweets_df))]
        
        tweets_df['Position'] = pos_list
        tweets_df['Party'] = party_list
        
        #print("GERÇEK",tweets_df.head())
        
        
        return tweets_df
            
        
        
    def insert_mongo(self, screen_name):
        #db.employees.find().pretty()
        
        count_before = self.records.count_documents({})
        print("Number of Records before insert:", count_before)
        twitterObj = self.createDF(screen_name)
        
        
        #sys.exit()
        
        tweets_dict = twitterObj.to_dict(orient='records')   
        try:
            self.records.insert_many(tweets_dict)
        except (OperationFailure, TypeError) as e:
            print(e)
            pass
            
        count_after = self.records.count_documents({})
        print("Final Number of Records:", count_after)
        
        self.alltweets = []        
            
    def twitter_credentials(self):
        consumer_key = self.cfg['consumer_key_2']
        consumer_secret = self.cfg['consumer_secret_2']
        access_token = self.cfg['access_token_2']
        access_secret = self.cfg['access_secret_2']

        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)

        self.api = tweepy.API(auth,  wait_on_rate_limit=True, wait_on_rate_limit_notify =True)
        print(self.api)
        
    def getTweets(self):
        self.twitter_credentials()
        
        df_read = pd.read_csv("Users_common.csv", sep = ";", index_col = False)
        print(df_read.head())
        
        #screen_name = 'joebiden'
        counter = 1
        for _, row in df_read.iterrows():
            print("Index:", counter, "User Name:", row['Username'])
            screen_name = row['Username'] 
            counter += 1
            
            try:
                user = self.records.find({'user':'{}'.format(screen_name)}).sort("_id")
                last_tweet = user[0]['ID']
                #all subsiquent requests use the max_id param to prevent duplicates
                new_tweets = self.api.user_timeline(screen_name = screen_name, count=100, since_id='{}'.format(last_tweet))
                logging.info('Tweets added for:{}'.format(screen_name))
                
            except IndexError:
                logging.info('Tweets added for new user {}'.format(screen_name))
                new_tweets = self.api.user_timeline(screen_name = screen_name, count=100)
            
            except tweepy.TweepError:
                logging.info('Sleeping for rate limit.')
                time.sleep(60)                  
                
            #save most recent tweets
            self.alltweets.extend(new_tweets)
            
            logging.info("...{} tweets downloaded.".format(len(self.alltweets)))
            
            self.insert_mongo(screen_name)

            

        
        
        
Tweets_Handler()
