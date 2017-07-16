import json


class WebMessage:
    def __init__(self, event, channel):
        self.params = {}
        self.data = {}
        self.event = event
        self.channel = channel

    def set_field(self, field, value):
        self.params[field] = value

    def __str__(self):
        tmp = {
            'event': self.event,
        }
        if self.channel:
            tmp['channel'] = self.channel
        if self.params:
            tmp['parameters'] = self.params
        if self.data:
            tmp['data'] = self.data

        return str(tmp).replace(' ', '')

    def to_string(self):
        return str(self)

    @staticmethod
    def parse(str: str) -> object:
        dict = json.loads(str)
        if isinstance(dict, list):
            dict = dict[0]
        event = None
        channel = None

        if 'event' in dict:
            event = dict['event']
        if 'channel' in dict:
            channel = dict['channel']

        if 'error_code' in dict:
            return ErrorMessage(dict['error_code'], channel)

        msg = WebMessage(event, channel)
        if 'parameters' in dict:
            msg.params = dict['params']
        if 'data' in dict:
            msg.data = dict['data']

        if channel == 'addChannel':
            if 'channel' in msg.data:
                msg.channel = msg.data['channel']
        elif msg.event == 'pong':
            msg.channel = 'ping'

        return msg


class ErrorMessage(WebMessage):
    def __init__(self, error_code, channel):
        self.params = {}
        self.data = {}
        self.event = error_code
        self.channel = channel
