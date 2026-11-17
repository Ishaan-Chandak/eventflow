from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import time

class Handler(BaseHTTPRequestHandler):
    status = 200
    received = []
    def do_POST(self):
        if self.path == '/control':
            length = min(int(self.headers.get('Content-Length', '0')), 1024)
            try:
                status = json.loads(self.rfile.read(length)).get('status')
                if status != 'timeout':
                    status = int(status)
                if status not in (200, 400, 500, 'timeout'):
                    raise ValueError('Unsupported status')
            except (ValueError, TypeError, json.JSONDecodeError):
                self.send_error(400)
                return
            Handler.status = status
            self.send_response(204)
            self.end_headers()
            return
        if self.path != '/webhook':
            self.send_error(404)
            return
        length = min(int(self.headers.get('Content-Length', '0')), 1048576)
        self.rfile.read(length)
        self.received.append(self.headers.get('X-EventFlow-Delivery-Id'))
        if self.status == 'timeout':
            time.sleep(6)
            return
        self.send_response(self.status)
        self.end_headers()
    def do_GET(self):
        if self.path == '/control':
            body = json.dumps({'status': self.status}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path != '/received':
            self.send_error(404)
            return
        body = json.dumps(self.received).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

HTTPServer(('0.0.0.0', 8082), Handler).serve_forever()
