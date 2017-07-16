import logging
from websocket import WebSocketApp

from websocketclient.channel import Channel
from websocketclient.error_handler import ErrorHandler
from websocketclient.web_message import WebMessage, ErrorMessage


class Dispatcher:
    logger = logging.getLogger('Dispatcher')

    def __init__(self, error_handler: ErrorHandler):
        self.channels = {}
        self.error_handler = error_handler

    def add_channel(self, channel: Channel):
        self.channels[channel.name] = channel

    def on_message(self, web_message: WebMessage):
        if isinstance(web_message, ErrorMessage):
            self.error_handler.on_error(web_message)
            pass

        name = web_message.channel

        if name in self.channels:
            self.channels[name].on_message(web_message)
        else:
            Dispatcher.logger.error('%s not found in channel', name)

    def on_connect(self, web_socket: WebSocketApp):
        for channel in self.channels.values():
            channel.on_connect(web_socket)
