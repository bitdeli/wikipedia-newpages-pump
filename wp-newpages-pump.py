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

def read_newest():
    try:
        return open('newest-processed').read().decode('utf-8')
    except:
        return ''

def write_newest(newest_processed):
    f = open('newest-processed', 'w')
    f.write(newest_processed.encode('utf-8'))
    f.close()

def parse_entry(entry):
    def field(fieldname):
        m = entry.find(True, {'class': fieldname})
        return m.string if m else None
    article = {}
    article['name'] = name = field('mw-newpages-pagename')
    article['time'] = field('mw-newpages-time')
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
    return name, article

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
    print urlopen(BITDELI_URL, event).read()

def pump(newest_processed):
    while True:
        try:
            group_key = int(time.time())
            first = None
            for name, article in entries():
                if name == newest_processed:
                    break
                if not first:
                    first = name
                send_to_bitdeli(article, group_key)
            if first:
                newest_processed = first
                write_newest(first)
        except Exception:
            traceback.print_exc()
        time.sleep(MINUTE)

if __name__ == '__main__':
    pump(read_newest())


