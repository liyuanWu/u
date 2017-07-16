from kdb.kdb_connection_engine import KDBConnectionEngine
from kdb.kdb_schema import KDBSchema, KDBType
from websocketclient.kdb_writer import KDBWriter, SpotDepthWriter
from websocketclient.logger import setup_logging
from websocketclient.channel import Channel
from websocketclient.dispatcher import Dispatcher
from websocketclient.error_handler import ErrorHandler
from websocketclient.ping_channel import PingChannel
from websocketclient.web_socket_wrapper import WebSocketWrapper


class WebSocketClient(object):
    def __init__(self):
        self.error_handler = ErrorHandler()
        self.dispatcher = Dispatcher(self.error_handler)
        self.web_socket_wrapper = WebSocketWrapper(self.dispatcher, self.error_handler)
        self.kdb_connection = KDBConnectionEngine()
        self.init_channels()
        self.web_socket_wrapper.start()

    def init_channels(self):
        self.dispatcher.add_channel(PingChannel())
        #currencies = ['btc', 'ltc', 'eth']
        currencies = ['ltc']
        for c in currencies:
            #self.dispatcher.add_channel(Channel('ok_sub_spotcny_{0}_ticker'.format(c)))
            #self.dispatcher.add_channel(Channel('ok_sub_spotcny_{0}_trades'.format(c)))
            table_name = "spot_depth_" + str(c)
            kdb_schema = KDBSchema(table_name)
            kdb_schema.add_column("timestamp", KDBType.TIMESTAMP)
            kdb_schema.add_column("bidask", KDBType.SYMBOL)
            kdb_schema.add_column("price", KDBType.FLOAT)
            kdb_schema.add_column("size", KDBType.FLOAT)
            if not self.kdb_connection.is_table_exist(kdb_schema.table_name):
                self.kdb_connection.create_table_by_schema(kdb_schema)
            kdb_writer = SpotDepthWriter(kdb_schema)
            self.dispatcher.add_channel(Channel('ok_sub_spot_{0}_depth'.format(c), kdb_writer, self.kdb_connection))


def main():
    client = WebSocketClient()



if __name__ == '__main__':
    setup_logging()
    main()
