import unittest 
from unittest.mock import patch
from unittest.mock import MagicMock
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
import socket
from threading import Thread
import kachok
from pathlib import PurePath
import time
#Borrowed from https://realpython.com/testing-third-party-apis-with-mock-servers/

class MockServerRequestHandler(BaseHTTPRequestHandler):
    counter=0
    def __init__(self, *args,**kwargs) -> None:
        
        super().__init__(*args,**kwargs)

    def do_POST(self):       
       
        # self.send_header("Content-Type","application/json")
        # We will reply to 6 requests with 429 and to 7th onwards with 200
        # # 2 4 8 16 32 64 
        # print(f"Counter is: {MockServerRequestHandler.counter}")
        mock_response=b"""{"name":"ResponseError","meta":{"body":"429 Too Many Requests /****/_search","statusCode":429,"headers":{"date":"Thu, 18 Nov 2021 18:35:30 GMT","content-type":"text/plain;charset=ISO-8859-1","content-length":"54","connection":"keep-alive","server":"Jetty(8.1.12.v20130726)"},"meta":{"context":null,"request":{"params":{"method":"POST","path":"/***/_search","body":{"type":"Buffer","data":[]},"querystring":"size=100&from=0&_source=id about 10 fields","headers":{"user-agent":"elasticsearch-js/7.10.0 (linux 4.14.248-189.473.amzn2.x86_64-x64; Node.js v16.13.0)","accept-encoding":"gzip,deflate","content-type":"application/json","content-encoding":"gzip","content-length":"294"},"timeout":30000},"options":{},"id":5379},"name":"elasticsearch-js","connection":{"url":"https://***/","id":"https://***/","headers":{},"deadCount":0,"resurrectTimeout":0,"_openRequests":0,"status":"alive","roles":{"master":true,"data":true,"ingest":true,"ml":false}},"attempts":0,"aborted":false}}}"""
        if MockServerRequestHandler.counter<6:
            self.send_response(429,"Too Many Requests")
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(mock_response)
            # print("Returning err")
        else:
            self.send_response(200,"Oki doki")
            self.end_headers()
            self.wfile.write(b"""{"errors":false}""")
            # print("Returning OK")
        MockServerRequestHandler.counter+=1
        return


class TestBackpressure(unittest.TestCase):

    def _get_free_port(self):
        s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
        s.bind(('localhost', 0))
        address, port = s.getsockname()
        s.close()
        return port

    def _startHTTPServer(self):
        self.mock_server_port = self._get_free_port()
        self.mock_server = HTTPServer(('localhost', self.mock_server_port), MockServerRequestHandler)
        # Start running mock server in a separate thread.
        # Daemon threads automatically shut down when the main process exits.
        self.mock_server_thread = Thread(target=self.mock_server.serve_forever)
        self.mock_server_thread.setDaemon(True)
        self.mock_server_thread.start()

    def setUp(self) -> None:
        MockServerRequestHandler.counter=0
        self._startHTTPServer()        
        return super().setUp()
    
    def test_backupressure(self):
        endpoint=f"http://localhost:{self.mock_server_port}"
        k=kachok.Kachok(endpoint)        
        fp=str(PurePath(PurePath(__file__).parent,"sample_json.json",))
        time.sleep=MagicMock()
        k.pumpJSONND("fakeindex",fp,progress=False)
        time.sleep.assert_called_with(32)
        # self.skipTest("yolo")

    def test_backupressure_large_batch(self):
        endpoint=f"http://localhost:{self.mock_server_port}"
        k=kachok.Kachok(endpoint)        
        fp=str(PurePath(PurePath(__file__).parent,"sample_json.json",))
        time.sleep=MagicMock()
        k.pumpJSONND("fakeindex",fp,progress=False,batchsize=6)
        time.sleep.assert_called_with(32)
        # self.skipTest("yolo")

    def tearDown(self) -> None:
        self.mock_server.shutdown()
        return super().tearDown()


if __name__ == '__main__':
    unittest.main()