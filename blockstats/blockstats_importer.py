import datetime
from concurrent.futures import ThreadPoolExecutor
import logging

from blockstats.blockstack_client import BlockstackClient
from blockstats import parser
from blockstats import downloader
from blockstats.timer import Timer
from bson import ObjectId


class BlockstatsImporter:

    def __init__(self, blockstack_node_url, database):
        self.blockstack_node_url = blockstack_node_url
        self.dao = database.storage()

    def import_all(self):
        snapshot_id = self.dao.save_snapshot(datetime.datetime.utcnow()).inserted_id
        logging.info('Snapdhot: %s', snapshot_id)
        self.import_identities(snapshot_id)

        identities = self.dao.get_identities(snapshot_id)
        self.import_profile_urls(identities)

        identities = self.dao.get_identities(snapshot_id)
        self.import_app_installs(identities, snapshot_id)

        self.dao.update_snapshot(snapshot_id, datetime.datetime.utcnow())

    def import_all_multithreaded(self, import_urls_threadcount, import_profiles_threadcount):
        snapshot_id = self.dao.save_snapshot(datetime.datetime.utcnow()).inserted_id
        logging.info('Snapdhot: %s', snapshot_id)
        self.import_identities(snapshot_id)

        self.import_profile_urls_multithreaded(snapshot_id, import_urls_threadcount)

        self.import_app_installs_multithreaded(snapshot_id, import_profiles_threadcount)

        self.dao.update_snapshot(snapshot_id, datetime.datetime.utcnow())

    def import_identities(self, snapshot_id):
        client = BlockstackClient(self.blockstack_node_url)

        timestamp = datetime.datetime.utcnow()
        names = client.download_all_names()
        subdomains = client.download_all_subdomains()

        identities = [dict(parser.parse_identity(address)) for address in names + subdomains]

        self.dao.save_identities(snapshot_id, timestamp, identities)

    def import_profile_urls_multithreaded(self, snapshot_id, threads=1):
        identities = self.dao.get_identities(snapshot_id)
        identity_batches = _batchify_by_address(identities, threads)
        pool = ThreadPoolExecutor(threads)
        futures = []
        for index, batch in enumerate(identity_batches):
            future = pool.submit(self.import_profile_urls, batch, index)
            futures.append(future)
        for future in futures:
            future.result()

    def import_profile_urls(self, identities, thread_id=1):
        client = BlockstackClient(self.blockstack_node_url)
        timer = Timer(len(identities), thread_id)
        timer.start()
        for identity in identities:
            self._import_profile_url(client, identity['snapshot_id'], identity['address'])
            timer.tick()
            timer.log()

    def _import_profile_url(self, client, snapshot_id, address):
        details = client.download_name_details(address)
        zonefile = None
        profile_url = None
        if details:
            zonefile = details.get('zonefile')
        if zonefile:
            profile_url = parser.extract_profile_url(zonefile)
        if profile_url:
            self.dao.update_profile_url(snapshot_id, address, profile_url)

    def import_app_installs_multithreaded(self, snapshot_id, threads=1):
        if isinstance(snapshot_id, str):
            snapshot_id = ObjectId(snapshot_id)
        identities = self.dao.get_identities(snapshot_id)
        identity_batches = _batchify_by_address(identities, threads)
        pool = ThreadPoolExecutor(threads)
        futures = []
        for index, batch in enumerate(identity_batches):
            future = pool.submit(self.import_app_installs, batch, snapshot_id, index)
            futures.append(future)
        for future in futures:
            future.result()

    def import_app_installs(self, identities, snapshot_id, thread_id=1):
        identities = [x for x in identities if x.get('profile_url')]
        timer = Timer(len(identities), thread_id)
        timer.start()
        for identity in identities:
            apps = None
            profile_json = None
            profile_url = identity['profile_url']
            is_person = None
            if profile_url:
                profile_json = downloader.download(profile_url)
            if profile_json:
                apps = parser.extract_apps_list(profile_json)
                is_person = parser.is_person(profile_json)
            self.dao.update_is_person(snapshot_id, identity['address'], is_person)
            if apps:
                self.dao.save_app_installations(datetime.datetime.utcnow(), snapshot_id, identity['address'], apps)
            timer.tick()
            timer.log()

def _batchify_by_address(arr, parts):
    batches = [[] for _ in range(parts)]
    for item in arr:
        batches[hash(item['address']) % parts].append(item)
    return batches
