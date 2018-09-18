from blockstats import downloader

class BlockstackClient:

    def __init__(self, node_url):
        self.node_url = node_url

    def download_all_names(self):
        return downloader.download_paged_array(f'{self.node_url}/v1/names')

    def download_all_subdomains(self):
        return downloader.download_paged_array(f'{self.node_url}/v1/subdomains')

    def download_name_details(self, name):
        return downloader.download(f'{self.node_url}/v1/names/{name}')
