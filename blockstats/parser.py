import re

def parse_identity(address):
    parts = address.split('.')
    number_of_parts = len(parts)
    if number_of_parts == 1:
        raise ValueError(f"Not a valid blockstack address: {address}")
    elif number_of_parts == 2:
        return {'address': address, 'namespace': parts[-1], 'name': parts[0]}
    else:
        return {'address': address, 'namespace': parts[-1],
                'name': '.'.join(parts[1:-1]), 'subdomain': parts[0]}

def extract_profile_url(zonefile):
    if not isinstance(zonefile, str):
        return None
    match = re.search(r'^_https?._tcp.*"(http.*)"$', zonefile.strip(), re.MULTILINE)
    return match.group(1) if match else None


def is_person(profile_json):
    try:
        return profile_json[0]['decodedToken']['payload']['claim']['@type'] == 'Person'
    except KeyError:
        return False

def extract_apps_list(profile_json):
    try:
        return list(profile_json[0]['decodedToken']['payload']['claim']['apps'])
    except KeyError:
        return None
