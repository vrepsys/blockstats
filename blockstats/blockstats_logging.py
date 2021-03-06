import sys
import logging
from datetime import datetime

def setup():
    logging.basicConfig(
        filename='logs/blockstats.{:%Y-%m-%d}.log'.format(datetime.now()),
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')

def write_stdouts_to_log():
    sys.stdout = StdLogger(logging.info)
    sys.stderr = StdLogger(logging.error)

class StdLogger:
    def __init__(self, logger):
        self.logger = logger

    def write(self, lines):
        for line in lines.rstrip().splitlines():
            self.logger(line.rstrip())

    def flush(self):
        pass
