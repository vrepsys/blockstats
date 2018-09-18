import responses

from blockstats import downloader

@responses.activate
def test_download_paged_array():
    def add_paged_response(page, response):
        responses.add(
            method='GET',
            url=f'http://x.yz/?page={page}',
            match_querystring=True,
            json=response
        )
    add_paged_response(0, ['1', '2'])
    add_paged_response(1, ['3', '4'])
    add_paged_response(2, [])

    downloaded_array = downloader.download_paged_array('http://x.yz')

    assert downloaded_array == ['1', '2', '3', '4']

@responses.activate
def test_download():
    responses.add(
        method='GET',
        url=f'http://x.yz/json',
        json={'hello':'hi'}
    )
    assert downloader.download('http://x.yz/json') == {'hello': 'hi'}

@responses.activate
def test_download_failure():
    responses.add(
        method='GET',
        url=f'http://x.yz/json',
        json={'error':'not found'},
        status=404
    )
    assert downloader.download('http://x.yz/json') is None
