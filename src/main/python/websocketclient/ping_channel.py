import threading

from websocket import WebSocketApp

from websocketclient.channel import Channel
from websocketclient.web_message import WebMessage


class PingChannel(Channel):
    INTERVAL = 30
    def __init__(self):
        self.name = 'ping'
        self.message_queue = []

    def on_connect(self, web_socket: WebSocketApp):
        self.send_ping(web_socket)

    def on_message(self, web_message: WebMessage):
        Channel.logger.info('%s receive message: %s', self.name, web_message)

    def send_ping(self, web_socket: WebSocketApp):
        msg = WebMessage('ping', None)
        web_socket.send(str(msg))
        threading.Timer(PingChannel.INTERVAL, self.send_ping, [web_socket]).start()




