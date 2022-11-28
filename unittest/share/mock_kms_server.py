# -*- coding: utf-8 -*-

import BaseHTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
import os
import socket
import ssl
import json
import random

script_home = os.path.dirname(os.path.abspath(__file__))
ip = "127.0.0.1"
port = 8888
response_data = """{
    "token": "ob_test1",
    "key": "5D6A57468605ADB17C065B3423E7112D",
    "hash": "E185FFAD6642A467BF8E4311086E80A9B9F6B55D6BF2FE6A13F935E0479FC558",
    "keyversion": "1",
    "success": true,
    "msg": ""
}
"""

class MyHandler(SimpleHTTPRequestHandler):
    def do_POST(self):
        self.data_string = self.rfile.read(int(self.headers['Content-Length']))

        self.end_headers()
        print self.data_string
        self.wfile.write(response_data)
        return

def main():
    print ("simple https server, address:%s:%d" % (ip, port))

    httpd = BaseHTTPServer.HTTPServer(('127.0.0.1', port), MyHandler)
    httpd.socket = ssl.wrap_socket(httpd.socket, certfile='./mock_kms.pem', server_side=True)
    httpd.serve_forever()

if __name__ == '__main__':
    main()
