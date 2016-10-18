from nltk.sentiment.vader import SentimentIntensityAnalyzer
import thread_simulator
import pandas as pd
from pymongo import MongoClient
import praw
from global_sc import sc
import global_config
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

mc = MongoClient()

def parseComment(reddit_comment):
    ps = TextBlob(reddit_comment.body).sentiment
    flair = reddit_comment.author_flair_css_class
    flair = 'None' if flair is None else flair.split()[0].rstrip('1234567890')
    fanbase_dict = global_config.FLAIRS[reddit_comment.subreddit.display_name]
    fanbase = str(fanbase_dict[flair])
    return (fanbase,
            [{'fanbase': fanbase,
              'created': int(reddit_comment.created),
              'polarity': ps.polarity,
              'subjectivity': ps.subjectivity,
              'text': str(reddit_comment.body)}])

def get_counts(input):
    if input[1] == []:
        return (input[0], [])
    df = pd.DataFrame(input[1]).sort('created', ascending=False).head(global_config.NUM_COMMENTS)
    d = dict(df.mean())
    d['count'] = len(input[1])
    d['created'] = df['created'].max()
    d['text'] = list(df['text'])
    d['fanbase'] = input[0]
    return (input[0], d)

def valid_flair(reddit_comment):
    flair = reddit_comment.author_flair_css_class
    flair = 'None' if flair is None else flair.split()[0].rstrip('1234567890')
    fanbase_dict = global_config.FLAIRS[reddit_comment.subreddit.display_name]
    return flair in fanbase_dict

class TextBlobProcessor():
    def __init__(self, thread_id):
        self.thread_id = thread_id
        mc['reddit']['vader_%s' % self.thread_id].delete_many({})
        r = praw.Reddit(user_agent='Tushar Ranjan DSI %s' % thread_id)
        submission = r.get_submission(submission_id=thread_id)
        if submission.subreddit.display_name in global_config.FLAIRS:
            initial_data = [
                (i,
                 [{'count': 0, 'created': 0, 'fanbase': i, 'text': [], 'polarity':0, 'subjectivity':0}])
                for i in set(global_config.FLAIRS[submission.subreddit.display_name].values())]
        else:
            initial_data = []
        mc['reddit']['vader'].update({'_id': thread_id}, {'title': submission.title}, upsert=True)
        sc.parallelize([1, 2, 3])
        self.rdd = sc.parallelize(initial_data)

    def __del__(self):
        sc = None
        mc['reddit']['vader_%s' % self.thread_id].delete_many({})

    def simulateThread(self, sleep_time=1, by_second=True):
        batch = 0
        sim = thread_simulator.ThreadSimulator(self.thread_id)
        fn = sim.streamCommentsBySecond if by_second else sim.streamComments
        for i in fn(sleep_time):
            temp_rdd = sc.parallelize(i).filter(valid_flair).map(parseComment).reduceByKey(lambda x, y: x + y)
            self.rdd = self.rdd.union(temp_rdd).reduceByKey(lambda x, y: x + y)
            for j in self.rdd.map(get_counts).collect():
                j[1]['batch'] = batch
                mc['reddit']['vader_%s' % self.thread_id].insert(j[1])
            batch += sleep_time