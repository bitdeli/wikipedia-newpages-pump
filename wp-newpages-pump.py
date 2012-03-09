import traceback
import json
import time
import os
import re

from urllib2 import Request, urlopen
from BeautifulSoup import BeautifulSoup

MINUTE = 60

WPNEW_URL = "http://en.wikipedia.org/w/index.php?title=Special:NewPages&offset=&limit=100"

BITDELI_URL = "https://in.bitdeli.com/events/i-04bac7a80799e8-ee380031"
BITDELI_AUTH = os.environ['BITDELI_AUTH']
TIME_FORMAT = '%H:%M, %d %B %Y'

def read_newest():
    try:
        return time.strptime(open('newest-processed').read(), TIME_FORMAT)
    except:
        return 0

def write_newest(newest_processed):
    f = open('newest-processed', 'w')
    f.write(time.strftime(TIME_FORMAT, newest_processed))
    f.close()

def parse_entry(entry):
    def field(fieldname):
        m = entry.find(True, {'class': fieldname})
        return m.string if m else None
    article = {}
    article['name'] = field('mw-newpages-pagename')
    tstamp = time.strptime(field('mw-newpages-time'), TIME_FORMAT)
    article['time'] = time.strftime('%Y-%m-%d %H:%M', tstamp)
    article['link'] = "http://en.wikipedia.org" + entry.find(True, 'mw-newpages-pagename')['href']
    user = field('mw-userlink')
    if user:
        article['user'] = user
        article['new_user'] = False
    else:
        article['user'] = field('new mw-userlink')
        article['new_user'] = True
    article['length'] = int(''.join(re.findall('[0-9]', field('mw-newpages-length'))))
    comment = entry.find(True, {'class': 'comment'})
    m = comment.find(True, {'class': 'mw-redirect'})
    if m:
        m.extract()
    article['comment'] = ' '.join(comment.findAll(text=True)).replace(' . ', '. ')\
                                                             .replace(' , ', ', ')\
                                                             .replace('  ', ' ').strip()
    return tstamp, article

def entries():
    req = Request(WPNEW_URL, headers={'user-agent': 'bitdeli-python'})
    page = BeautifulSoup(urlopen(req).read())
    page.find(True, {'id': 'Newpages_change'}).extract()
    for entry in page.find(True, {'id': 'bodyContent'}).findAll('li'):
        try:
            yield parse_entry(entry)
        except Exception:
            traceback.print_exc()

def send_to_bitdeli(article, group_key):
    event = json.dumps({'auth': BITDELI_AUTH,
                        'group_key': group_key,
                        'object': article})
    print  event #urlopen(BITDELI_URL, event).read()

def pump(newest_processed):
    while True:
        try:
            print 'fetch'
            group_key = int(time.time())
            new_newest = newest_processed
            for tstamp, article in entries():
                print 'tstamp', time.strftime(TIME_FORMAT, tstamp)
                if tstamp == newest_processed:
                    break
                new_newest = max(tstamp, new_newest)
                send_to_bitdeli(article, group_key)
            newest_processed = new_newest
            write_newest(newest_processed)
        except Exception:
            traceback.print_exc()
        time.sleep(MINUTE)

if __name__ == '__main__':
    pump(read_newest())


