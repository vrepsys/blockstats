from pymongo import MongoClient

class MongoAcessDriver:
    def __init__(self):
        self.client = MongoClient()

    def reset(self):
        self.client.drop_database('blockstats-test')

    def get_identity_addresses(self, snapshot_id):
        identities = list(self.client['blockstats-test'].identities.find({'snapshot_id': snapshot_id}))
        return ([identity['address'] for identity in identities])

    def get_all_app_installations(self, snapshot_id):
        return list(self.client['blockstats-test'].app_installations.find({'snapshot_id': snapshot_id}))

    def get_all_snapshots(self):
        return list(self.client['blockstats-test'].snapshots.find())

    def get_identity(self, snapshot_id, address):
        return self.client['blockstats-test'].identities.find_one({'snapshot_id': snapshot_id, 'address': address})

    def get_profile_url(self, snapshot_id, address):
        return self.get_identity(snapshot_id, address).get('profile_url')

    def get_is_person(self, snapshot_id, address):
        return self.get_identity(snapshot_id, address).get('is_person')
