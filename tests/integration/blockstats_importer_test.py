# pylint: disable=redefined-outer-name
import json
import responses
import pytest

from blockstats.mongo import Mongo
from blockstats.blockstats_importer import BlockstatsImporter
from tests.fakes.fake_blockstack_node import FakeBlockstackNode, name_details
from tests.integration.mongo_access_driver import MongoAcessDriver

@pytest.fixture
def mongo():
    mongo_driver = MongoAcessDriver()
    mongo_driver.reset()
    return mongo_driver

@pytest.fixture
def blockstack_node():
    return FakeBlockstackNode(
        url='https://blockstacknode.xyz',
        names=['valdemaras.id'],
        subdomains=['valrepsys.blockstack.id'],
        name_details=[
            name_details(
                address='valdemaras.id',
                zone_file='$ORIGIN valdemaras.id\n$TTL 3600\n_http._tcp\tIN\tURI\t10\t1\t\"https://abc.xyz/profile.json\"\n\n'),
            name_details(
                address='valrepsys.blockstack.id',
                zone_file='abcd'
            )]
        )

def set_up_user_profile(url, json_profile_path):
    with open(json_profile_path, "r") as valid_profile:
        responses.add(
            method='GET',
            url=url,
            match_querystring=True,
            json=json.load(valid_profile)
        )

@responses.activate
def test_import_all(mongo, blockstack_node):
    set_up_user_profile(
        url='https://abc.xyz/profile.json',
        json_profile_path='tests/data/valid_profile.json'
    )

    mongodb = Mongo('blockstats-test')
    importer = BlockstatsImporter(blockstack_node.url, mongodb)
    importer.import_all_multithreaded(4, 4)

    snapshot = mongo.get_all_snapshots()[0]
    snapshot_id = snapshot['_id']
    assert 'start' in snapshot
    assert 'finish' in snapshot
    assert mongo.get_identity_addresses(snapshot_id) == ['valdemaras.id', 'valrepsys.blockstack.id']
    assert mongo.get_profile_url(snapshot_id, 'valdemaras.id') == 'https://abc.xyz/profile.json'
    assert mongo.get_is_person(snapshot_id, 'valdemaras.id') is True
    assert mongo.get_all_app_installations(snapshot_id)[0]['apps'] == [
        'https://app.graphitedocs.com',
        'http://localhost:8080',
        'https://www.stealthy.im',
        'http://fupio.com']
