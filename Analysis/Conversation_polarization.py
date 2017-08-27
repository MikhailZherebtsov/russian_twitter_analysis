import MySQLdb, sys, logging
import pandas as pd
#-------------------------------------------------------

class ConvoMatrix:
    '''
    This script creates the file 'Polarization_matrices.xlsx' - which is reports
    the matrix of polarization by community. The Retweets, the mentions and the
    replies are pulled, and then the crosstabs are saved.

    input:
    - sample table -- to limit only those users who talked about about a specific topic

    NOTE: when a reply happens, this is also captured in the mention data, hence
    the reply should be subtracted from the mentioned to get the strictly mentioned
    result. This is not programmed into this script and can be done manually in excel. 
    '''
    def __init__(self, sample):
        self.__log__()
        self.logger.info('\n')
        self.logger.info('Initializing ConvoMatrix object for the network')
        self.logger.info('Connecting to db')
        self.conn = MySQLdb.connect(host="***",
                                    user="***",
                                    password="***",
                                    db="***",
                                    charset="utf8mb4")
        self.c = self.conn.cursor()
        self.sample = sample
        
    def __log__(self):
        #initiate logging
        logging.basicConfig(filename='ConvoMatrix.log',
                            level=logging.DEBUG,
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')
        self.logger = logging.getLogger(__name__)
        
    #------------------------------------------------------------------
    def check_all_convos(self):
        self.logger.info('Checking all convos and gathering data')
        print('getting convos: rts and mentions...')
        self.c.execute("SELECT twtMaster.userid, mentionsMaster.userid \
                        FROM twtMaster \
                        INNER JOIN mentionsMaster \
                        ON twtMaster.twtid=mentionsMaster.twtid;")
        dfm1 = pd.DataFrame(list(self.c.fetchall()), columns=['userid1', 'userid2'])
        self.c.execute("SELECT rtMaster.userid, twtMaster.userid \
                        FROM rtMaster \
                        INNER JOIN twtMaster \
                        ON rtMaster.rttwtid=twtMaster.twtid;")
        dfrt1 = pd.DataFrame(list(self.c.fetchall()), columns=['userid1', 'userid2'])
        self.c.execute("SELECT tusers.userid, tsamp.imrev \
                        FROM usersMaster tusers \
                        INNER JOIN %s tsamp \
                        ON tusers.userid=tsamp.userid"%self.sample)
        dfu = pd.DataFrame(list(self.c.fetchall()), columns=['userid', 'community'])
        dfm2 = pd.merge(dfm1,
                        dfu,
                        how='inner',
                        left_on='userid1',
                        right_on='userid')
        dfm3 = pd.merge(dfm2,
                        dfu,
                        how='inner',
                        left_on='userid2',
                        right_on='userid')
        del dfm3['userid1']
        del dfm3['userid2']
        del dfm3['userid_x']
        del dfm3['userid_y']
        dfrt2 = pd.merge(dfrt1,
                         dfu,
                         how='inner',
                         left_on='userid1',
                         right_on='userid')
        dfrt3 = pd.merge(dfrt2,
                         dfu,
                         how='inner',
                         left_on='userid2',
                         right_on='userid')
        del dfrt3['userid1']
        del dfrt3['userid2']
        del dfrt3['userid_x']
        del dfrt3['userid_y']
        dfrt4 = pd.crosstab(dfrt3.community_x, dfrt3.community_y, margins=True)
        dfm4 = pd.crosstab(dfm3.community_x, dfm3.community_y, margins=True)
        return dfrt4, dfm4


    def get_replies(self):
        self.logger.info('Checking replies and gathering data')
        print('getting replies...')
        self.c.execute("SELECT twtid, inreplytotwtid FROM repliesmaster;")
        dfreps = pd.DataFrame(list(self.c.fetchall()),columns=['twtid','inreplytotwtid'])
        self.c.execute("SELECT twtid, userid FROM twtMaster;")
        dftwts = pd.DataFrame(list(self.c.fetchall()), columns=['twtid','userid'])
        self.c.execute("SELECT userid, imrev FROM custom_network_small;")
        dfsamp = pd.DataFrame(list(self.c.fetchall()), columns=['userid','community'])
        dfr1 = pd.merge(dfreps,
                        dftwts,
                        how='inner',
                        on='twtid')
        dfr2 = pd.merge(dfr1,
                        dftwts,
                        how='inner',
                        left_on='inreplytotwtid',
                        right_on='twtid')
        dfr3 = pd.merge(dfr2,
                        dfsamp,
                        how='inner',
                        left_on='userid_x',
                        right_on='userid')
        dfr4 = pd.merge(dfr3,
                        dfsamp,
                        how='inner',
                        left_on='userid_y',
                        right_on='userid')
        dfr5 = dfr4.drop(dfr4.columns[[0,1,2,3,4,5,7]], axis=1)
        dfr6 = pd.crosstab(dfr5.community_x, dfr5.community_y, margins=True)
        return dfr6

    def save(self, dfs_as_list, tab_names_as_list):
        print('saving...')
        writer = pd.ExcelWriter('Polarization_matrices.xlsx')
        i = 0
        for df in dfs_as_list:
            print('saving %s'%tab_names_as_list[i])
            df.to_excel(writer, tab_names_as_list[i])
            i += 1
        writer.save()
        print('done saving polarization matrix!!')
        
def main():
    print('starting...')
    convo = ConvoMatrix()
    dfrt, dfm = convo.check_all_convos()
    dfr = convo.get_replies()
    convo.save([dfrt, dfm, dfr], ['rt','mention','reply'])


#------------------------------------------------------
if __name__ == '__main__':
    main()
