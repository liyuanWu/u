import datetime
import logging

from kdb.kdb_row import KDBRow
from kdb.kdb_schema import KDBSchema, KDBSymbol, KDBInt, KDBTimestamp, KDBFloat


class KDBWriter(object):
    def __init__(self, kdb_schema: KDBSchema):
        self.logger = logging.getLogger(__name__)
        self.kdb_schema = kdb_schema

    def convert_event_to_kdb_row(self, event: dict):
        pass


class SpotDepthWriter(KDBWriter):
    def convert_event_to_kdb_row(self, data: dict):
        kdb_rows = []
        timestamp_int = int(data['timestamp'])
        timestamp = datetime.datetime.fromtimestamp(timestamp_int/1000)
        asks = data['asks']
        bids = data['bids']

        if asks:
            for ask in asks:
                kdb_row = KDBRow(self.kdb_schema)
                kdb_row.prepare_statement(KDBTimestamp(timestamp), KDBSymbol("ask"), KDBFloat(ask[0]), KDBFloat(ask[1]))
                kdb_rows.append(kdb_row)
        if bids:
            for bid in bids:
                kdb_row = KDBRow(self.kdb_schema)
                kdb_row.prepare_statement(KDBTimestamp(timestamp), KDBSymbol("bid"), KDBFloat(bid[0]), KDBFloat(bid[1]))
                kdb_rows.append(kdb_row)

        return kdb_rows
