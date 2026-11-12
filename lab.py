import json
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def fetch(url, headers=None):
    request = Request(url, headers=headers or {})
    try:
        response = urlopen(request, timeout=2)
    except HTTPError as error:
        response = error
    with response:
        return response.code, dict(response.headers.items()), response.read()


def get_json(url):
    status, headers, body = fetch(url)
    if status != 200:
        raise ValueError(f'来源返回状态 {status}')
    return json.loads(body)


@contextmanager
def serving(source):
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            with lock:
                status, headers, body = source.handle(self.path, self.headers)
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={'poll_interval': 0.01}, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class StreamSource:
    """演示用分块事件来源。

    与 Source 不同，handle_stream 按块产出字节并可在中途断开，用来制造
    跨块 UTF-8、半帧断线等真实网络情形。Source 本身保持不变。
    """

    def __init__(self, chunks, status=200, headers=None, fail_after=None,
                 chunk_delay=0.0):
        self.chunks = list(chunks)
        self.status = status
        self.headers = headers or {'Content-Type': 'text/event-stream'}
        # fail_after: 发出前 N 个块后直接断开连接（模拟故障）。
        self.fail_after = fail_after
        self.chunk_delay = chunk_delay
        self.requests = []

    def handle(self, path, headers):
        route = path.split('?', 1)[0]
        if route != '/events':
            return 404, {}, b''
        self.requests.append(headers.get('Last-Event-ID', ''))
        return self.status, self.headers, b''.join(self.chunks)

    def handle_stream(self, path, headers):
        route = path.split('?', 1)[0]
        if route != '/events':
            yield 404, {}, [b'']
            return
        self.requests.append(headers.get('Last-Event-ID', ''))
        yield self.status, self.headers, None
        emitted = 0
        for index, chunk in enumerate(self.chunks):
            if self.fail_after is not None and index >= self.fail_after:
                return
            yield None, None, chunk
            emitted += 1
            if self.chunk_delay:
                time.sleep(self.chunk_delay)


@contextmanager
def serving_stream(source):
    """按块回写、可中途断连的演示服务器。"""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_GET(self):
            for status, headers, chunk in source.handle_stream(self.path, self.headers):
                if status is not None:
                    self.send_response(status)
                    for name, value in headers.items():
                        self.send_header(name, value)
                    self.end_headers()
                    continue
                if chunk:
                    self.wfile.write(chunk)
                    self.wfile.flush()

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={'poll_interval': 0.01}, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
