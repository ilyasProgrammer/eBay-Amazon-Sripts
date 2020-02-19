# -*- coding: utf-8 -*-

import SocketServer
from BaseHTTPServer import BaseHTTPRequestHandler
import ctypes  # An included library with Python install.
import socket
from urlparse import urlparse, parse_qs
ZEBRA_PRINTER_IP = '10.0.0.55'
ZEBRA_PRINTER_PORT = 9100


class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if urlparse(self.path).path == '/print':
                parameters = parse_qs(urlparse(self.path).query)
                print_data = parameters.get("data")
                self.zebra_print(print_data[0])
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                page = '''<html><body>OK</body></html>'''
                self.wfile.write(page)
            else:
                self.send_response(400)
        except Exception as e:
            ctypes.windll.user32.MessageBoxW(0, e, u'Error', 1)

    def zebra_print(self, print_data):
        # ctypes.windll.user32.MessageBoxW(0, u'Ok', u'Title', 1)
        print print_data  # TODO check spec chars convers
        # mysocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # try:
        #     mysocket.connect((ZEBRA_PRINTER_IP, ZEBRA_PRINTER_PORT))  # connecting to host
        #     mysocket.send(bytes(print_data))  # using bytes
        #     mysocket.close()  # closing connection
        # except Exception as e:
        #     print e
        #     ctypes.windll.user32.MessageBoxW(0, e, u'Error', 1)


httpd = SocketServer.TCPServer(("", 9876), MyHandler)
httpd.serve_forever()
