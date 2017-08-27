# -*- coding: utf-8 -*-

import re, sys, time, json, unicodedata, tweepy, collections
from tweepy import OAuthHandler
from re import sub
from MySQL_CategorizeNSave import CategorizeNsave

'''
This script collects data using the REST API and saves it into the database
using the CategorizeNsave object in the MySQL_CategorizeNSave.py file:
- Keywords/topics are specified in the 'searchstring' variable
- db_table specifies the name of the table you want to create for this sample
'''

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
searchstring = "***" #enter the keyword string you want to search for
db_table = 'tbl_%s'%"***" #enter the name of the table you want to create in the db

saver = CategorizeNsave(db_table)

#----------
def Initiate_search():
    global tweet
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
    return True
 
if __name__ == '__main__': 
    Initiate_search()
