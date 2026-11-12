import json
from urllib.parse import urlsplit

class Source:
    def __init__(self, fixture):
        self.fixture=fixture
        self.seen_ids=[]

    def handle(self, path, headers):
        route=urlsplit(path).path
        if route=='/snapshot':
            payload={'cut':self.fixture['cut'],'devices':self.fixture['snapshot']}
            return 200, {'Content-Type':'application/json'},json.dumps(payload).encode()
        if route!='/events':
            return 404,{},b''
        cursor=headers.get('Last-Event-ID','')
        self.seen_ids.append(cursor)
        if cursor and cursor.isdecimal() and int(cursor)<self.fixture['cut']:
            return 409,{'Content-Type':'application/json'},b'{"snapshot":"/snapshot"}'
        return 200,{'Content-Type':'text/event-stream'},''.join(self.fixture['chunks']).encode()

def byte_chunks(fixture, size=7):
    if size<1:
        raise ValueError('分块长度必须为正')
    raw=''.join(fixture['chunks']).encode()
    return [raw[offset:offset+size] for offset in range(0,len(raw),size)]
