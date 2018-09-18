import bson

class StatsQueries:
    def __init__(self, db):
        self._db = db

    def get_latest_snapshot(self):
        return list(self._db.snapshots.aggregate([
            {'$sort': {'finish': -1}},
            {'$limit': 1}
        ]))[0]

    def get_app_counts(self, snapshot_id):
        return list(self._db.app_installations.aggregate([
            {'$match': {'snapshot_id': snapshot_id}},
            {'$unwind': '$apps'},
            {'$group': {
                '_id': '$apps',
                'count': {'$sum': 1}
            }
            },
            {'$match': {'_id': {'$not': bson.regex.Regex('localhost')}}},
            {'$project': {'name': '$_id', 'count': 1}},
            {'$sort': {'count': -1}}
        ]))

    def get_total_address_counts(self):
        return list(self._db.identities.aggregate(
            ADDRESS_COUNTS_AGGREGATIONS
        ))

    def get_domain_counts(self):
        return list(self._db.identities.aggregate([
            {'$match': {'name': {'$ne': None}, 'subdomain': None}},
            *ADDRESS_COUNTS_AGGREGATIONS
        ]))

    def get_subdomain_counts(self):
        return list(self._db.identities.aggregate([
            {'$match': {'subdomain': {'$ne': None}}},
            *ADDRESS_COUNTS_AGGREGATIONS
        ]))

    def get_person_counts(self):
        return list(self._db.identities.aggregate([
            {'$match': {'is_person': True}},
            *ADDRESS_COUNTS_AGGREGATIONS
        ]))

    def get_app_time_series(self, apps):
        return list(self._db.app_installations.aggregate([
            {'$unwind': '$apps'},
            {
                '$group': {
                    '_id': {
                        'app': '$apps',
                        'snapshot': '$snapshot_id'
                    },
                    'count': {'$sum': 1}
                }
            },
            {'$match': {'_id': {'$not': bson.regex.Regex('localhost')}}},
            {'$match': {'_id.app': {'$in': apps}}},
            {'$project': {'snapshot': '$_id.snapshot',
                          'app.name': '$_id.app', 'app.count': '$count'}},
            {'$group': {
                '_id': {'snapshot': '$_id.snapshot'},
                'apps': {'$push': '$app'}
            }
            },
            {'$lookup': {
                'from': 'snapshots',
                'localField': '_id.snapshot',
                'foreignField': '_id',
                'as': 'snapshot'
            }
            },
            {'$project': {'_id': 0, 'apps': 1, 'snapshot': {
                '$arrayElemAt': ["$snapshot", 0]}}},
            {'$project': {'values': '$apps', 'date': {'$dateToString': {
                'format': '%Y-%m-%d', 'date': '$snapshot.start'}}}},
            {'$sort': {'date': 1}}
        ]))

    def get_total_installs_time_series(self):
        return list(self._db.app_installations.aggregate([
            {'$unwind': '$apps'},
            {'$match': {'apps': {'$not': bson.regex.Regex('localhost')}}},
            {'$group': {
                '_id': {
                    'snapshot_id': '$snapshot_id'
                },
                'count': {'$sum': 1}
            }
            },
            {'$lookup': {
                'from': 'snapshots',
                'localField': '_id.snapshot_id',
                'foreignField': '_id',
                'as': 'snapshot'
            }
            },
            {'$project': {'_id': 0, 'count': 1, 'snapshot': {
                '$arrayElemAt': ["$snapshot", 0]}}},
            {'$project': {'value': '$count', 'date': {'$dateToString': {
                'format': '%Y-%m-%d', 'date': '$snapshot.start'}}}},
            {'$sort': {'date': 1}}
        ]))

    def get_localhost_installs_time_series(self):
        return list(self._db.app_installations.aggregate([
            {'$unwind': '$apps'},
            {'$match': {'apps': bson.regex.Regex('localhost')}},
            {'$group': {
                '_id': {
                    'snapshot_id': '$snapshot_id',
                    'username': '$username'
                }
            }
            },
            {'$group': {
                '_id': {
                    'snapshot_id': '$_id.snapshot_id'
                },
                'count': {'$sum': 1}
            }
            },
            {'$lookup': {
                'from': 'snapshots',
                'localField': '_id.snapshot_id',
                'foreignField': '_id',
                'as': 'snapshot'
            }
            },
            {'$project': {'_id': 0, 'count': 1, 'snapshot': {
                '$arrayElemAt': ["$snapshot", 0]}}},
            {'$project': {'value': '$count', 'date': {'$dateToString': {
                'format': '%Y-%m-%d', 'date': '$snapshot.start'}}}},
            {'$sort': {'date': 1}}
        ]))


ADDRESS_COUNTS_AGGREGATIONS = [
    {
        '$group': {
            '_id': {'snapshot': '$snapshot_id'},
            'value': {'$sum': 1}
        }
    },
    {
        '$lookup': {
            'from': 'snapshots',
            'localField': '_id.snapshot',
            'foreignField': '_id',
            'as': 'snapshot'
        }
    },
    {'$project': {'_id': 0, 'value': 1, 'snapshot': {
        '$arrayElemAt': ["$snapshot", 0]}}},
    {'$project': {'value': 1, 'date': {'$dateToString': {
        'format': '%Y-%m-%d', 'date': '$snapshot.start'}}}},
    {'$sort': {'date': 1, 'value': -1}}
]
