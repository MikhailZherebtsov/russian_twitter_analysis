# -*- coding: utf-8 -*-

import re, sys, time, json, unicodedata, tweepy, collections
from tweepy import OAuthHandler
from re import sub
from MySQL_CategorizeNSave import CategorizeNsave

#------------------------------------------------------------------------------
#Enter the keys of your twitter applicaiton to connect to the API
ckey = "***"        #consumer key
csecret = "***"     #consumer secret
atoken = "***"      #access token
asecret = "***"     #access secret

auth = tweepy.OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

api = tweepy.API(auth)
#------------------------------------------------------------------------------
searchstring = "***"    #enter the keyword string you want to search for
db_table = "***"        #enter the name of the table you want to create in the db

class Search_topic:
    '''
    This script collects data using the REST API and saves it into the database
    using the CategorizeNsave object in the MySQL_CategorizeNSave.py file:
    - Keywords/topics are specified in the 'searchstring' variable
    - db_table specifies the name of the table you want to create for this sample
    '''
    def __init__(self, db_table):
        self.db_table = db_table
        self.saver = CategorizeNsave('tbl_%s'%db_table)

    def __inster_sample_into_event_table__(self):
        self.saver.c.execute("INSERT IGNORE INTO event_summary (\
                                table_name_in_db, sample_type) \
                                VALUES ('tbl_%s, REST);"%self.db_table)
        self.saver.conn.commit()
        
    def Initiate_search():
        while True:
            try:
                print('working...')
                for tweet in tweepy.Cursor(api.search, q=searchstring, lang="ru").items(): 
                    print(tweet.created_at.tzinfo, str(tweet.created_at))
                    saver.raw_input(tweet)
            except tweepy.error.TweepError:
                print('waiting...')
                time.sleep(60*5)
                continue
            
     
if __name__ == '__main__': 
    Search_topic(db_table)
