# -*- coding: utf-8 -*-
import MySQLdb, tweepy, time
from tweepy import OAuthHandler
#-----------------------------------------------------------------------------------
#Enter the keys of your twitter applicaiton to connect to the API
ckey = "***"        #consumer key
csecret = "***"     #consumer secret
atoken = "***"      #access token
asecret = "***"     #access secret

auth = tweepy.OAuthHandler(ckey, twtkeys.csecret)
auth.set_access_token(atoken, asecret)
api = tweepy.API(auth)
#-----------------------------------------------------------------------------------


class Friendships:
    '''
    This class performs a search for friends given a database input. Process:
    (1) Searches db to find list of ids of master users
    (2) Performs REST API request for friendships
    (3) Compares each users's friends with the other users in sample
    (4) Saves directed relationships under linksTable: userid1, userid2
    '''
    def __init__(self):
        self.__log__()
        self.logger.info('\n')
        self.logger.info('Initializing Friendship collection')
        self.logger.info('Connecting to db')
        self.__connet_to_db__()
        self.mastertime = time.time()
        self.todo_list = self.gather_users()
        self.check_friendships()
    #----supporting functions------------------------------------
    def __connet_to_db__(self):
        #connect to database server
        self.conn = MySQLdb.connect(host="***",
                                    user="***",
                                    password="***",
                                    db="***",
                                    charset="utf8mb4")
        self.c = self.conn.cursor()

    def __log__(self):
        #initiate logging
        logging.basicConfig(filename='Friendship_collection.log',
                            level=logging.DEBUG,
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')
        self.logger = logging.getLogger(__name__)
    #--------------main functions----------------------------------
    def gather_users(self):
        '''
        This function gets users to check friendships from the specified db under connection
        '''
        self.logger.info('Collects users for which to collect friendships')
        self.c.execute("SElECT userid FROM usersMaster;")
        result = self.c.fetchall()
        users = list(each[0] for each in result)
        return users
       
    def get_friendships(self, checkuser):
        '''
        This function contains the api call to get friends_ids - and rotate through pages
        and also calls the save_links function
        '''
        i = 1
        for page in tweepy.Cursor(self.api.friends_ids, user_id=checkuser).pages():
            print('Getting %s user, page %d'%(checkuser, i))
            for user in page:
                if str(user) in self.users_list:
                    self.savelist.append((checkuser, str(user)))
            time.sleep(60)
            i += 1

    def save_links(self, user1, user2):
        '''
        This function saves the friendship edge
        '''
        self.c.execute("INSERT OR IGNORE INTO linksMaster (userid1, userid2) VALUES (%s, %s)",
                       (user1, user2))
        self.conn.commit()

    def check_friendships(self):
        '''
        Master function to start iterating through all the users and saving their data
        '''
        self.logger.info('Starting collection of friendships')
        self.timestart = time.time()
        self.i = 1
        for checkuser in self.todo_list:
            self.logger.info('Collecting data for user %s'%checkuser)
            self.savelist = []
            while True:
                try:
                    self.get_friendships(checkuser)
                    break
                except tweepy.error.RateLimitError:
                    print('reached limit, waiting...')
                    time.sleep(60*5)
                    continue
            for each in self.savelist:
                self.save_links(each[0], each[1])
            self.c.execute("UPDATE usersMaster SET done=1 WHERE userid='%s'"%checkuser)
            self.conn.commit()
            print('Done %s; %d users of %d; %d minutes since start'%(checkuser, self.i, len(self.todo_list), round((time.time()-self.mastertime)/60,0)))
            self.i += 1


            
#==============================================================
if __name__ == '__main__':
    friendchecker = Friendships()
