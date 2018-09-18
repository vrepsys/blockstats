import pymongo
from pymongo import MongoClient
from blockstats.blockstats_dao import BlockstatsDao
from blockstats.stats_queries import StatsQueries

class Mongo:
    def __init__(self, database, host='localhost'):
        self._mongo_client = MongoClient(host=host)
        setup_database(self._mongo_client[database])
        self._storage = BlockstatsDao(self._mongo_client[database])
        self._stats_queries = StatsQueries(self._mongo_client[database])

    def storage(self):
        return self._storage

    def stats_queries(self):
        return self._stats_queries

def setup_database(database):
    database.identities.create_index([("address", pymongo.DESCENDING), ("snapshot_id", pymongo.DESCENDING)], unique=True)
    