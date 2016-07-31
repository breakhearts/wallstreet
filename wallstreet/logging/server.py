from __future__ import absolute_import
try:
    import socketserver
except:
    #wrap python2
    import SocketServer as socketserver
import pickle
import struct
import logging
import logging.handlers
import logging.config
import select
from wallstreet.logging import config as log_config
from wallstreet import config


logging.config.dictConfig(log_config.LOGGING)


class LogRecordStreamHandler(socketserver.StreamRequestHandler):
    """
    handler for a streaming logging request
    """
    def handle(self):
        while True:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack('>L', chunk)[0]
            chunk = self.connection.recv(slen)
            while len(chunk) < slen:
                chunk = chunk + self.connection.recv(slen - len(chunk))
            obj = self.unpickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handle_log_record(record)

    def unpickle(self, data):
        return pickle.loads(data)

    def handle_log_record(self, record):
        if self.server.logname is not None:
            name = self.server.logname
        else:
            name = record.name
        logger = logging.getLogger(name)
        logger.handle(record)


class LogServer(socketserver.ThreadingTCPServer):
    """
    simple log server
    """
    allow_reuse_address = True

    def __init__(self, host='localhost',
                 port=logging.handlers.DEFAULT_TCP_LOGGING_PORT, handler=LogRecordStreamHandler):
        socketserver.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = 0
        self.timeout = 1
        self.logname = None

    def start(self):
        abort = 0
        while not abort:
            rd, wr, ex = select.select([self.socket.fileno()], [], [], self.timeout)
            if rd:
                self.handle_request()
            abort = self.abort

if __name__ == "__main__":
    s = LogServer(config.get("log_server", "host"), config.get_int("log_server", "port"))
    s.start()
