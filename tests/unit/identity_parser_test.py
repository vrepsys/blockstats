import json

import pytest

from blockstats import parser

def test_parse_subdomain_identity():
    assert parser.parse_identity('val.id.blockstack') == \
     {'address': 'val.id.blockstack', 'namespace': 'blockstack', 'name': 'id', 'subdomain': 'val'}

def test_parse_name_identity():
    assert parser.parse_identity('id.blockstack') == \
     {'address': 'id.blockstack', 'namespace': 'blockstack', 'name': 'id'}

def test_parse_long_identity():
    assert parser.parse_identity('val.one.two.three.blockstack') == \
     {'address': 'val.one.two.three.blockstack', 'namespace': 'blockstack',
      'name': 'one.two.three', 'subdomain': 'val'}

def test_parse_invalid_identity():
    with pytest.raises(ValueError):
        assert parser.parse_identity('blockstack')

@pytest.mark.parametrize("zonefile,expected_profile_url", [
    ('$ORIGIN hello.id\n$TTL 3600\n_http._tcp URI 10 1 "https://gaia.blockstack.org/hub/123/0/profile.json"\n',
     'https://gaia.blockstack.org/hub/123/0/profile.json'),
    ('$ORIGIN hello.id\n$TTL 3600\n_http._tcp IN URI 10 1 "https://blockstack.s3.amazonaws.com/woah.id"',
     'https://blockstack.s3.amazonaws.com/woah.id')
])
def test_extract_profile_url(zonefile, expected_profile_url):
    assert parser.extract_profile_url(zonefile) == expected_profile_url

def test_parse_apps_list():
    with open("tests/data/valid_profile.json", "r") as valid_profile:
        profile_json = json.load(valid_profile)
        assert parser.extract_apps_list(profile_json) == [
            'https://app.graphitedocs.com',
            'http://localhost:8080',
            'https://www.stealthy.im',
            'http://fupio.com']

@pytest.mark.parametrize("profile_json", [[{}], {}])
def test_parse_apps_list_failure(profile_json):
    assert parser.extract_apps_list(profile_json) is None

def test_is_person():
    with open("tests/data/valid_profile.json", "r") as valid_profile:
        profile_json = json.load(valid_profile)
        assert parser.is_person(profile_json) is True
