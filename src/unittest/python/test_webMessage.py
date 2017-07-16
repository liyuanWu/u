from unittest import TestCase
from websocketclient.web_message import WebMessage


class TestWebMessage(TestCase):

    def test_to_str(self):
        w = WebMessage('event', 'channel')
        w.set_field('a', 'b')
        print(w.to_string())
        print(str(w))

    def test_parse(self):
        income_response = '''
        {
            "channel":"ok_spotcny_cancel_order",
            "data":{
                "order_id":125433027,
                "result":true
            }
        }
        '''
        msg = WebMessage.parse(income_response)
        print(msg)

