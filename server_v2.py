import socket, socketserver, http.server
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import cgi
import json
import time
import argparse
import requests
from multiprocessing import Process
from socketserver import ThreadingMixIn
import ssl

import embeddingService
import indexDatabase

hostName = "localhost"

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--threads", help="Number of threads", default=1, type=int)
parser.add_argument("-p", "--port", help="Port number", default=1240, type=int)
args = parser.parse_args()

N_THREADS = args.threads

STACK_SIZE = args.threads

PORT_STACK = []
for i in range(N_THREADS):
    PORT_STACK.append(str(1241 + i))

lock = threading.Lock()


class RequestHandler(BaseHTTPRequestHandler):
    def _send_response(self, message):
        self.send_response(200)
        self.send_header('Content-type', 'application/octet-stream')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(message)

    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })
        document = form.getfirst("document")

        port = None
        while port is None:
            with lock:
                global STACK_SIZE
                if len(PORT_STACK) > 0:
                    port = PORT_STACK.pop()

            if port is None:
                time.sleep(0.01)

        message = requests.post('http://localhost:' + port, data={"document": document}).content
        PORT_STACK.append(port)
        self._send_response(message)

def main(port=1240):
    addr = ('', port)
    sock = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(addr)
    sock.listen(1000)

    class Thread(threading.Thread):
        def __init__(self, i):
            threading.Thread.__init__(self)
            self.i = i
            self.daemon = True
            self.start()
        def run(self):
            httpd = http.server.HTTPServer(addr, RequestHandler, False)
            print("Starting baseHTTPServer", self.i)

            httpd.socket = sock
            httpd.server_bind = self.server_close = lambda self: None

            httpd.serve_forever()
    [Thread(i) for i in range(N_THREADS)]
    print("Running")
    time.sleep(9e9)

# main(args.port)

"""
def handle_requests(server):
    server.serve_forever()

server_address = ('', args.port)  # Server runs on port 1240
httpd = HTTPServer(server_address, RequestHandler)

workers = []
for _ in range(N_THREADS):
    worker = Process(target=handle_requests, args=(httpd,))
    worker.start()
    workers.append(worker)

for worker in workers:
    worker.join()
"""

class ThreadingSimpleServer(ThreadingMixIn,HTTPServer):
    pass

def main2(port):
    webServer = ThreadingSimpleServer(('', port), RequestHandler)
    # webServer.socket = ssl.wrap_socket(webServer.socket)  # keyfile='./privkey.pem',certfile='./certificate.pem', server_side=True)
    print("Server started http://%s:%s" % ('', port))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")

main2(args.port)
