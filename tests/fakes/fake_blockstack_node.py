import responses

class FakeBlockstackNode:

    def __init__(self, url, names=None, subdomains=None, name_details=None):
        self.url = url

        if names is None:
            names = []
        if subdomains is None:
            subdomains = []

        self.add_names_response(0, names)
        if names:
            self.add_names_response(1, [])

        self.add_subdomains_response(0, subdomains)
        if subdomains:
            self.add_subdomains_response(1, [])

        if name_details is not None:
            for (address, details) in name_details:
                self._add_name_details_response(address, details)

    def add_names_response(self, page, response):
        self._add_paged_get_response('names', page, response)

    def add_subdomains_response(self, page, response):
        self._add_paged_get_response('subdomains', page, response)

    def _add_name_details_response(self, address, details):
        responses.add(
            method='GET',
            url=f'{self.url}/v1/names/{address}',
            json=details
        )

    def _add_paged_get_response(self, path, page, response):
        responses.add(
            method='GET',
            url=f'{self.url}/v1/{path}?page={page}',
            match_querystring=True,
            json=response
        )

def name_details(address, zone_file):
    return (address, {'zonefile': zone_file})
