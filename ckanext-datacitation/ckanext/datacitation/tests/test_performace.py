import unittest
import csv


from datacitation.backend.postgres import VersionedDatastorePostgresqlBackend
from ckanext.datastore.backend.postgres import get_write_engine


class VersionedDatastoreTest(unittest.TestCase):

    def setUp(self):
        self.backend= VersionedDatastorePostgresqlBackend()
        self.engine = get_write_engine()

    def test_create_performance(self):
        with open('/home/hakson/Downloads/test_datas/100.csv') as f:
            data_dict = [{k: int(v) for k, v in row.items()}
                      for row in csv.DictReader(f, skipinitialspace=True)]

            context={}
            context['connection'] = self.engine.connect()

        self.backend.create(context,data_dict)
