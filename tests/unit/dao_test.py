# pylint: disable=redefined-outer-name
import datetime

import pytest

from pymongo import MongoClient
from blockstats.blockstats_dao import BlockstatsDao
from blockstats import mongo

@pytest.fixture
def dao():
    client = MongoClient()
    client.drop_database('blockstats-test')
    mongo.setup_database(client['blockstats-test'])
    return BlockstatsDao(client['blockstats-test'])

def test_save_identities(dao):
    timestamp = datetime.datetime.utcnow()
    identities = [{'address': '111.id', 'name': '111', 'namespace': 'id'},
                  {'address': 'hello.blockstack.id', 'name': 'blockstack', 'namespace': 'id', 'subdomain': 'hello'}]

    dao.save_identities('12345', timestamp, identities)

    timestamp_rounded_micros = timestamp.replace(microsecond=0)
    assert dao.read_all_identities() == [
        {'snapshot_id': '12345',
         'timestamp' : timestamp_rounded_micros,
         'address': '111.id',
         'namespace': 'id',
         'name': '111'},
        {'snapshot_id': '12345',
         'timestamp' : timestamp_rounded_micros,
         'address': 'hello.blockstack.id',
         'name': 'blockstack',
         'namespace': 'id',
         'subdomain': 'hello'}
    ]

def test_save_app_installations(dao):
    timestamp = datetime.datetime.utcnow()

    dao.save_app_installations(timestamp, 'abcd', 'valdemaras.id', ['app1', 'app2'])

    timestamp_rounded_micros = timestamp.replace(microsecond=0)
    assert dao.read_all_app_installations() == [
        {'timestamp' : timestamp_rounded_micros, 'snapshot_id': 'abcd', 'username': 'valdemaras.id', 'apps': ['app1', 'app2']}
    ]

def test_update_profile_url(dao):
    timestamp = datetime.datetime.utcnow()
    identities = [{'address': '111.id', 'name': '111', 'namespace': 'id'}]

    dao.save_identities(snapshot_id='1234', timestamp=timestamp, identities=identities)
    dao.update_profile_url(snapshot_id='1234', address='111.id', profile_url='https://testurl.com')

    assert dao.read_all_identities()[0].get('profile_url') == 'https://testurl.com'

def test_update_expire_block(dao):
    timestamp = datetime.datetime.utcnow()
    identities = [{'address': '111.id', 'name': '111', 'namespace': 'id'}]

    dao.save_identities(snapshot_id='1234', timestamp=timestamp, identities=identities)
    dao.update_expire_block(snapshot_id='1234', address='111.id', expire_block=100)

    assert dao.read_all_identities()[0].get('expire_block') == 100

def test_update_is_person(dao):
    timestamp = datetime.datetime.utcnow()
    identities = [{'address': '111.id', 'name': '111', 'namespace': 'id'}]

    dao.save_identities(snapshot_id='1234', timestamp=timestamp, identities=identities)
    dao.update_is_person(snapshot_id='1234', address='111.id', is_person=True)

    assert dao.read_all_identities()[0].get('is_person') is True


def test_snapshots(dao):
    start = datetime.datetime.utcnow()
    dao.save_snapshot(start)

    snapshot = dao.read_all_snapshots()[0]
    assert snapshot['start'] == start.replace(microsecond=0)
    assert '_id' in snapshot
    assert 'finish' not in snapshot

    finish = datetime.datetime.utcnow()
    dao.update_snapshot(snapshot['_id'], finish)

    snapshot = dao.read_all_snapshots()[0]
    assert snapshot['finish'] == finish.replace(microsecond=0)

def test_force_remove_snapshot(dao):
    start = datetime.datetime.utcnow()
    timestamp = datetime.datetime.utcnow()
    dao.save_snapshot(start)
    dao.save_snapshot(start)

    all_snapshots = dao.read_all_snapshots()
    snapshot1 = all_snapshots[0]
    snapshot1_id = snapshot1['_id']
    snapshot2 = all_snapshots[1]
    snapshot2_id = snapshot2['_id']

    identities = [{'address': '111.id'}]

    dao.save_identities(snapshot1_id, timestamp, identities)
    dao.save_identities(snapshot2_id, timestamp, identities)

    dao.save_app_installations(timestamp, snapshot1_id, 'x', ['x'])
    dao.save_app_installations(timestamp, snapshot2_id, 'x', ['x'])


    dao.force_remove_snapshot(snapshot1_id)

    all_snapshots = dao.read_all_snapshots()
    assert len(all_snapshots) == 1
    assert all_snapshots[0] == snapshot2

    assert len(dao.get_identities(snapshot1_id)) == 0
    assert len(dao.get_identities(snapshot2_id)) == 1

    assert len(dao.get_app_installations(snapshot1_id)) == 0
    assert len(dao.get_identities(snapshot2_id)) == 1
