import logging
import pymongo
from bson import ObjectId

class BlockstatsDao:

    def __init__(self, db):
        self._db = db

    def save_identities(self, snapshot_id, timestamp, identities):
        timestamp = timestamp.replace(microsecond=0)
        identities = [{'timestamp': timestamp, 'snapshot_id': snapshot_id,
                       **identity} for identity in identities]
        try:
            self._db.identities.insert_many(identities, ordered=False)
        except pymongo.errors.BulkWriteError as error:
            logging.error(
                "Bulk write error when saving identities: %s", error.details)

    def read_all_identities(self):
        return list(self._db.identities.find({}, {'_id': False}))

    def get_all_snapshots(self):
        return list(self._db.snapshots.find({}).sort([('start', -1)]))

    def get_identities(self, snapshot_id):
        return list(self._db.identities.find({'snapshot_id': snapshot_id}, {'_id': False}))

    def update_profile_url(self, snapshot_id, address, profile_url):
        self._db.identities.find_one_and_update(
            {'snapshot_id': snapshot_id, 'address': address},
            {"$set": {"profile_url": profile_url}})

    def update_is_person(self, snapshot_id, address, is_person):
        self._db.identities.find_one_and_update(
            {'snapshot_id': snapshot_id, 'address': address}, {"$set": {"is_person": is_person}})

    def save_app_installations(self, timestamp, snapshot_id, username, apps):
        timestamp = timestamp.replace(microsecond=0)
        self._db.app_installations.insert_one(
            {'timestamp': timestamp, 'snapshot_id': snapshot_id, 'username': username, 'apps': apps})

    def read_all_app_installations(self):
        return list(self._db.app_installations.find({}, {'_id': False}))

    def get_app_installations(self, snapshot_id):
        return list(self._db.app_installations.find({'snapshot_id': snapshot_id}, {'_id': False}))

    def save_snapshot(self, start):
        start = start.replace(microsecond=0)
        return self._db.snapshots.insert_one({'start': start})

    def read_all_snapshots(self):
        return list(self._db.snapshots.find())

    def update_snapshot(self, _id, finish):
        finish = finish.replace(microsecond=0)
        self._db.snapshots.find_one_and_update(
            {'_id': _id}, {'$set': {'finish': finish}})

    def force_remove_snapshot(self, snapshot_id):
        if isinstance(snapshot_id, str):
            snapshot_id = ObjectId(snapshot_id)
        self._db.snapshots.delete_one({'_id': snapshot_id})
        self._db.identities.delete_many({'snapshot_id': snapshot_id})
        self._db.app_installations.delete_many({'snapshot_id': snapshot_id})

    def remove_app_installs(self, snapshot_id):
        if isinstance(snapshot_id, str):
            snapshot_id = ObjectId(snapshot_id)
        self._db.app_installations.delete_many({'snapshot_id': snapshot_id})
