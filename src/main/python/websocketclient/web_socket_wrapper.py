import websocket
import logging

from websocketclient.dispatcher import Dispatcher
from websocketclient.error_handler import ErrorHandler
from websocketclient.web_message import WebMessage


class WebSocketWrapper():
    logger = logging.getLogger("WebSocketWrapper")
    ADDRESS = 'wss://real.okcoin.cn:10440/websocketclient/okcoinapi'  # The remote host
    USERNAME = 'fcd8af68-2f1e-4d9e-816f-15c91b42474c'
    PASSWORD = '90ec6db50fde54c10e537e562c438700'

    @staticmethod
    def on_close(ws):
        WebSocketWrapper.logger.info("### closed ###")

    def __init__(self, dispatcher: Dispatcher, error_handler: ErrorHandler):
        self.socket_app = websocket.WebSocketApp(WebSocketWrapper.ADDRESS,
                                                 on_message=self.on_message,
                                                 on_error=self.on_error,
                                                 on_close=self.on_close)
        self.web_socket = None
        self.dispatcher = dispatcher
        self.error_handler = error_handler

    def start(self):
        if self.logger.isEnabledFor(logging.DEBUG):
            websocket.enableTrace(True)

        self.socket_app.on_open = self.on_open
        self.socket_app.run_forever()

    def on_message(self, ws, message):
        WebSocketWrapper.logger.debug("Receive message from web socket: %s", message)
        msg = WebMessage.parse(message)
        self.dispatcher.on_message(msg)

    def on_error(self, ws, error):
        self.error_handler.on_error(error)

    def on_open(self, ws, *args):
        self.web_socket = ws
        self.dispatcher.on_connect(self.web_socket)

    def get_web_socket(self):
        return self.web_socket
