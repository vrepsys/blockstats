import logging
import requests

def download(url, timeout=10):
    logging.info('Downloading. url: %s', url)
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code != 200:
            logging.info('Request failed with status code: %s', resp.status_code)
            return None
        return resp.json()
    except Exception as e:
        logging.info(e)
        return None

def download_paged_array(url):
    page = 0
    all_names = []
    while True:
        names = _download_page(url, page)
        if names != []:
            all_names += names
            page += 1
        else:
            return all_names

def _download_page(url, page):
    logging.info('Downloading. url: %s, page: %s', url, page)
    resp = requests.get(url, {'page': page}, timeout=10)
    return resp.json()
