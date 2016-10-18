import pickle
import time

class ThreadSimulator():
    def __init__(self, thread_id):
        self.thread_id = thread_id
        self.p = pickle.load(open('pickled/%s.p' % thread_id, 'r'))
        self.sorted_posts = sorted(self.p[0], key=lambda x: x.created)
        self.timestamps = sorted(list(set([i.created for i in self.sorted_posts])))

    def streamCommentsBySecond(self, sleep_time = 1):
        for i in self.timestamps:
            j = 0
            while j < len(self.sorted_posts) and self.sorted_posts[j].created == i:
                j += 1
            return_vals = [q for q in self.sorted_posts[:j] if q.author_flair_css_class is not None]
            self.sorted_posts = self.sorted_posts[j:]
            yield sorted(return_vals, key=lambda x: x.created)
            time.sleep(sleep_time)

    def streamComments(self, sleep_time=5):
        begin = int(self.timestamps[0])
        end = int(self.timestamps[-1])
        sorted_ts = sorted([i for i in xrange(begin, end + 1, sleep_time)])
        i = sorted_ts[0]
        while i <= sorted_ts[-1]:
            ts_range = [j for j in xrange(i, i + sleep_time)]
            j = 0
            while j < len(self.sorted_posts) \
                    and self.sorted_posts[j].created <= ts_range[-1] \
                    and self.sorted_posts[j].created >= ts_range[0]:
                j += 1
            return_vals = [q for q in self.sorted_posts[:j] if q.author_flair_css_class is not None]
            self.sorted_posts = self.sorted_posts[j:]
            yield sorted(return_vals, key=lambda x: x.created)
            time.sleep(5)
            i += sleep_time