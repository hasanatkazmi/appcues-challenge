#!/usr/bin/env python

import BaseHTTPServer
from urlparse import parse_qs

class AppcuesServer(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        if 'Content-Length' not in self.headers:
            # https://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.4
            self.send_response(411)
            return
        if self.path != '/increment':
            self.send_response(400)
            return
        data_length = self.headers['Content-Length']
        data_length = int(data_length)
        data = self.rfile.read(data_length)
        content = parse_qs(data)
        keys = content.keys()
        if 'key' not in keys or 'value' not in keys:
            # bad request
            self.send_response(400)
            return
        key, value = content['key'][0], content['value'][0]
        value = int(value)
        print key, value
        self.send_response(200)


if __name__ == '__main__':
    server_port = 3333
    server = BaseHTTPServer.HTTPServer(('', server_port), AppcuesServer)
    print "Starting Server on port {port}".format(port=server_port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print "Shutting down the server"
        server.socket.close()
