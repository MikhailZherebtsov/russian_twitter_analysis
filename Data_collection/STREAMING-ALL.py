# -*- coding: utf-8 -*-

import tweepy, time
from MySQL_CategorizeNSave import CategorizeNsave
import queue, threading
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
#------------------------------------------------------------------------------
#Enter the keys of your twitter applicaiton to connect to the API
consumer_key = "***"            #consumer key
consumer_secret = "***"         #consumer secret
access_token = "***"            #access token
access_token_secret = "***"     #access secret
#------------------------------------------------------------------------------

class StdOutListener(StreamListener):
    '''
    This is an object that collects live tweets pushed by the Twitter Streaming
    API. The only specified filter applied to the stream is the russian language.
    No tweets in other languages (as categorized by the Twitter algorithm) will
    be saved.

    No table name needs to be specified for this script as a default table is
    created with the ids of all the statuses captured in this streaming sample.

    Note:
    This is also a multi-thread object, it operates the save and stream functions
    separately in order to manage workflow on a PC better. If there is a large spike
    of statuses, a queue is created to normalize and save at a later time. This
    minimizes the risk that the Twitter API disconnects the connection due to the
    computer that is used is too slow to categorize and save all incoming statuses.
    '''
    def __init__(self):
        self.saver = CategorizeNsave()
    
    def on_status(self, status):
        while True:
            try:
                if status.lang == 'ru':
                    #only specified filter here is the russian language
                    print(str(status.created_at))
                    q.put(status)
                break
            except Exception as e:
                if e == 503:
                    print(str(e), ', server unavailible, waiting...')
                    time.sleep(30)
                    continue
                if e == 420:
                    print('420 error, waiting')
                    time.sleep(60)
                    continue
            except:
                print(str(e), type(e), len(e))
                time.sleep(15)
                continue
        return True

    def on_timeout(self):
        time.sleep(15)
        return True

    def on_ReadTimeoutError(self):
        return True

    def on_disconnect(self, notice):
        time.sleep(30)
        return True

    def on_exception(self, exception):
        return True

    def on_disconnect(self, notice):
        time.sleep(15)
        return True

    def on_AttributeError(self, error):
        time.sleep(15)
        return True

    def on_TypeError(self, error):
        time.sleep(15)
        return True

    def on_error(self, status):
        print(status)
        time.sleep(15)
        return True

def saverfn(q):
    print('saver thread running...')
    while True:
        if q.empty() == False:
            result = q.get()
            self.saver.raw_input(result)
        else:
            time.sleep(15)

def streamer(q):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    while True:
        try:
            stream = Stream(auth, StdOutListener())
            stream.sample(languages=["ru"])
        except (TypeError, AttributeError):
            continue
        except:
            stream.disconnect()
            print('disconnected... will reconnect in 30')
            time.sleep(30)
            continue
            

if __name__ == '__main__':
    q = queue.Queue()

    worker_save = threading.Thread(target=saverfn, name='save-thread', args=(q,))
    worker_stream = threading.Thread(target=streamer, name='stream-thread', args=(q,))

    worker_save.start()
    worker_stream.start()

    q.join()
