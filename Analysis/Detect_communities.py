import community, time
import pickle
from collections import deque
import pandas as pd
from igraph import *
import MySQLdb, sys, logging
from dateutil import parser

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)
#=====================================================================================
class Community_Detector:
    '''
    ------------------
     - this script creates the communities and saves the community back to
     the sever.

    Input:
     - list of events to check (by event id)

    Steps:
    (1) connects to the server, extracts all the 'done' users for the event
    chosen.
    (2) connects to the same done users and extracts their frienships
    (3) creates the network, determines communities, and modularity
    (4) saves the modularity back to the event table
    (5) saves the community (as imrev) back to the event users table
    (6) a pickle of the newtork is saved as well if option is selected.
    -------------------
    '''
    def __init__(self, samples_to_do='All', write_into_db=True, export_gml=True, pickle=False):
        print('starting...')
        self.__logging__()
        self.logger.info('Starting the Community_Detector class...')
        self.write_into_db = write_into_db
        self.export_gml = export_gml
        self.pickle = pickle
        try:
            #Specify the necessary connection info to your MySQL server environment
            self.conn = MySQLdb.connect(host="***",
                            user="***",
                            password="***",
                            db="***",
                            charset="utf8mb4")
            self.c = self.conn.cursor()
            print('connection to db made...')
        except Exception as e:
            print('erred in connection to database server, exiting...')
            self.logger.error('Alert:',exc_info=True)
            sys.exit()
        #verify the input of the samples to do
        self.do_samples = self.__verify_input__(samples_to_do)
        self.Main()

    def __logging__(self):
        #initiates logging
        logging.basicConfig(filename='Community_detector.log',
                            level=logging.DEBUG,
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')
        self.logger = logging.getLogger(__name__)

    def __verify_input__(self, samples_to_do):
        #verify input for the samples to do against those in the database
        self.c.execute("SELECT id, table_name_in_db FROM event_summary \
                        WHERE done IS NULL AND friendship_group IS NOT NULL;")
        raw = self.c.fetchall()
        self.events = {each[1]:each[0] for each in raw}
        if samples_to_do == 'All':
            do_samples = self.events
        elif type(samples_to_do) is list and len(samples_to_do) > 1:
            do_samples = [each for each in samples_to_do if each in self.events]
        elif len(samples_to_do) == 1 and type(samples_to_do) is str:
            if samples_to_do in self.events:
                do_samples = list(samples_to_do)
            else:
                print('chose sample table not found in database, exiting...')
                sys.exit()
        return do_samples
    
    def __collect_data__(self, sample):
        #collect the data
        self.logger.info('collecting data for %s sample'%sample)
        self.c.execute("SELECT \
                            t1.userid2, \
                            t1.userid1 \
                        FROM linksMaster t1 \
                        INNER JOIN users%s t2 \
                        ON t1.userid1=t2.userid;"%sample)
        dflinks_raw = pd.DataFrame(list(self.c.fetchall()),
                                   columns=['userid1',
                                            'userid2'])
        self.c.execute("SELECT userid FROM users%s WHERE done=1;"%sample)
        dfusers = pd.DataFrame([each[0] for each in self.c.fetchall()],
                               columns=['userid'])
        self.c.execute("SELECT userid, username FROM usersMaster;")
        self.username_dict = {each[0]:each[1] for each in self.c.fetchall()}
        dflinks2 = pd.merge(dflinks_raw,
                            dfusers,
                            how='inner',
                            left_on='userid1',
                            right_on='userid')
        dflinks3 = pd.merge(dflinks2,
                            dfusers,
                            how='inner',
                            left_on='userid2',
                            right_on='userid')
        del dflinks3['userid_x']
        del dflinks3['userid_y']
        self.logger.info('done collecting data for %s sample'%sample)
        return dfusers, dflinks3

    def __recreate_edges__(self, dflinks, dfids):
        '''
        As the igraph network is created using nodes on an integer range and not
        the userids that are given, the below is a way to add edges to the network
        by reclassifying the iteger range of each vertex with the userid of the user
        '''
        dfe1 = pd.merge(dflinks,
                        dfids,
                        how='inner',
                        left_on='userid1',
                        right_on='userid')
        dfe2 = pd.merge(dfe1,
                        dfids,
                        how='inner',
                        left_on='userid2',
                        right_on='userid')
        dfe3 = dfe2.drop(['userid1','userid2','userid_x','userid_y'], axis=1)
        edges = dfe3.values.tolist()
        return edges

    #-----------------------------------------------------------------
    def Main(self):
        '''
        This is the main function of the class. Once called, ititerates
        through all the chosen sample tables to do, detects communites, and
        writes the imrev, as well as the modularity into the db + pickles the
        graph if it was selected
        '''
        for sample in self.do_samples:
            dfusers, dflinks = self.__collect_data__(sample)
            users = dfusers['userid'].values.tolist()
            self.logger.info('%s, creating graph and load users'%sample)
            self.g = Graph(directed=True)
            print('first populate nodes...')
            i = 1
            for user in users:
                self.g.add_vertex(user)
                print('{}, populating users: {:.3}% done'.format(sample, i/len(users)*100))
                i += 1
            i = 1
            if self.export_gml == True:
                self.logger.info('%s, adding usernames to the graph for export'%sample)
                i = 0
                for vertex in self.g.vs:
                    self.g.vs[vertex.index]['usernname'] = self.username_dict[vertex['name']]
                    print('{}, putting in usernames into the graph: {:.3}%'.format(sample,
                                                                                  i/len(self.g.vs)*100))
                    i += 1
            i = 0
            id_defin = []
            while i < len(users):
                id_defin.append((self.g.vs[i]['name'], i))
                print('{}, getting vertex attributes to make id defin: {:.3}% done'.format(sample,
                                                                                           i/len(users)*100))
                i += 1
            dfiddef = pd.DataFrame(id_defin, columns=['userid', 'id'])
            edges = self.__recreate_edges__(dflinks, dfiddef)
            print('%s, edges obtained'%sample)
            timestarted = time.time()
            self.g.add_edges(edges)
            print('%s, edges added, that took %f'%(sample, time.time()-timestarted))
            self.logger.info('%s, network created, calc communities'%sample)
            self.im = self.g.community_infomap()
            i = 0
            for group in self.im:
                print('%s, write community %d/%d'%(sample, i,len(self.im)))
                for vertex in group:
                    self.g.vs[vertex]['im'] = i
                i += 1
            print('%s, Write in to db: %s'%(sample, self.write_into_db))
            print('%s, Export gml: %s'%(sample, self.export_gml))
            if self.export_gml == True:
                self.logger.info('%s, export gml of network chosen'%sample)
                print('%s, exporting network'%sample)
                self.g.write_gml(sample+'_graph.gml')
            if self.pickle==True:
                self.logger.info('Pickling...')
                pickle.dump({'graph':self.g,
                             'im':self.im},
                            open('%s_pickle.p'%sample, 'wb'))
            if self.write_into_db == True:
                self.logger.info('%s, write imrev of network into event_summary'%sample)
                self.c.execute("UPDATE event_summary SET imrev_modularity=%f \
                                WHERE table_name='%s';"%(self.im.modularity, sample))
                self.conn.commit()
                self.logger.info('%s, create dictionary of values'%sample)
                i = 0
                self.d = {}
                while i < self.g.vcount():
                    self.d[self.g.vs[i]['name']] = {}
                    self.d[self.g.vs[i]['name']]['imrev'] = self.g.vs[i]['im']
                    i += 1
                i = 0
                self.logger.info('%s, write imrev into db'%sample)
                for user in self.d:
                    print('{}, updating database with community attributes: {:.3}%'.format(sample,
                                                                                           i/len(self.d)*100))
                    self.c.execute("UPDATE users%s SET imrev=%d WHERE userid='%s'"%(sample,
                                                                                    self.d[user]['imrev'],
                                                                                    user))
                    self.conn.commit()
                    i += 1
            self.c.execute("UPDATE event_summary SET done=1 WHERE id='%s';"%self.events[sample])
            self.conn.commit()
            print('done sample:',sample)
            self.logger.info('Sample %s completed'%sample)
        print('All done!!!')


                
if __name__ == "__main__":
    CD = Community_Detector()
