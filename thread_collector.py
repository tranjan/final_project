import praw
import pickle
import sys
import time

r = praw.Reddit(user_agent='Tushar Ranjan DSI')

REDDIT_THREADS = [
    '4lsdm2',
    '4lsyrs',
    '4082u3',
    '51ugn8',
    '2ugt8x',
    '2ugkac',
    '2ugdyh',
    '2ugyjr',
    '56vf22',
    '5715fx',
    '4hsdaa',
    '577k6u',
    '43kttt',
    '4i2sa5'
]

def get_comments(submission_id):
    global r
    submission = r.get_submission(submission_id=submission_id, comment_sort='new', params={'depth': 1})
    if type(submission.comments[-1]) == praw.objects.MoreComments:
        posts = submission.comments[:-1]
        new_comments = submission.comments[-1].children
        i = 0
        while i < len(new_comments):
            a = time.time()
            posts += r.get_info(thing_id=['t1_%s' % j for j in new_comments[i:i + 1000]])
            i += 1000
            print i, time.time() - a
    else:
        posts = submission.comments
    return sorted(posts, key=lambda x: x.created), submission

def write_to_pickle_file(submission_id):
    pickle.dump(get_comments(submission_id), open('pickled/%s.p' % submission_id, 'wb'))

if __name__ == '__main__':
    for t in REDDIT_THREADS:
        print 'Collecting %s' % t
        write_to_pickle_file(t)