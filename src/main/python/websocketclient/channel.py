import logging
from websocket import WebSocketApp

from kdb.kdb_connection_engine import KDBConnectionEngine
from websocketclient.kdb_writer import KDBWriter
from websocketclient.web_message import WebMessage

class Channel:
    logger = logging.getLogger('Channel')

    def __init__(self, name: str, kdb_writer: KDBWriter, kdb_connection_engine: KDBConnectionEngine):
        self.web_socket = None
        self.name = name
        self.kdb_writer = kdb_writer
        self.kdb_connection = kdb_connection_engine
        self.message_queue = []

    def on_message(self, web_message: WebMessage):
        Channel.logger.info('%s receive message: %s', self.name, web_message)
        if 'result' not in web_message.data:
            kdb_rows = self.kdb_writer.convert_event_to_kdb_row(web_message.data)
            for kdb_row in kdb_rows:
                self.kdb_connection.insert_row(kdb_row)
        #self.message_queue.append(web_message)

    def on_connect(self, web_socket: WebSocketApp):
        self.web_socket = web_socket
        msg = WebMessage('addChannel', self.name)
        self.web_socket.send(str(msg))
