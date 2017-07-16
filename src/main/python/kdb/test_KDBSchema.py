from unittest import TestCase

from kdb.kdb_schema import KDBSchema, KDBType


class TestKDBSchema(TestCase):
    def test_columns(self):
        kdb_schema = KDBSchema('table_name')
        kdb_schema.add_column("col1", KDBType.SYMBOL)
        kdb_schema.add_column("col2", KDBType.STRING)
        print(kdb_schema.columns())
