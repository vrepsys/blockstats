import argparse
from argparse import RawTextHelpFormatter

from blockstats.blockstats_importer import BlockstatsImporter
from blockstats.mongo import Mongo
from blockstats.stats import Stats
from blockstats import blockstats_logging
from blockstats import snapshots_printer

COMMANDS_HELP = """
import - imports data from a Blockstack node. Can take hours.
list-snapshots - lists previous and unfinished imports
remove-snapshot - removes a single import (a snapshot) and all its data. Requires --snapshot.
get-stats - prints out statistical data in json format
get-users-by-app - print all usernames that have a specified app intalled
""".strip()

def main():
    blockstats_logging.setup()

    parser = argparse.ArgumentParser(
        formatter_class=RawTextHelpFormatter,
        description='blockstats')
    parser.add_argument(
        'command',
        help=COMMANDS_HELP, choices=[
            'import', 'list-snapshots', 'remove-snapshot',
            'get-stats', 'get-app-counts-csv', 'get-historic-app-counts-csv', 'get-users-by-app'
        ])
    parser.add_argument(
        '--data-history',
        help="data json to be merged with stats from the database when doing get-stats.")
    parser.add_argument(
        '--snapshot',
        help='snapshot id, required when removing a snapshot')
    parser.add_argument(
        '--mongohost',
        help='MongoDB URI (default: mongodb://localhost:27017/blockstats)', default='mongodb://localhost:27017/blockstats')
    parser.add_argument(
        '--mongodb',
        help='mongo database name (default: blockstats)', default='blockstats')
    parser.add_argument(
        '--blockstacknode',
        help='blockstack node (default: https://core.blockstats.org)', default='https://core.blockstack.org')
    parser.add_argument(
        '--userdetails_threads',
        help='Number of threads to use when downloading user details from the blockstack node (default: 1)', default=1, type=int)
    parser.add_argument(
        '--userprofiles_threads',
        help='Number of threads to use when downloading user profiles (default: 1)', default=1, type=int)
    parser.add_argument(
        '--app',
        help='app name for serching usernames by app (default: https://dpage.io)', default='https://dpage.io')

    args = parser.parse_args()

    mongodb = Mongo(args.mongodb, args.mongohost)
    importer = BlockstatsImporter(args.blockstacknode, mongodb)

    if args.command == 'import':
        blockstats_logging.write_stdouts_to_log()
        importer.import_all_multithreaded(args.userdetails_threads, args.userprofiles_threads)    
    if args.command == 'import-app-installs' and args.snapshot:
        blockstats_logging.write_stdouts_to_log()
        importer.import_app_installs_multithreaded(args.snapshot, args.userprofiles_threads)
    elif args.command == 'list-snapshots':
        snapshots_printer.print_snapshots(mongodb.storage().get_all_snapshots())
    elif args.command == 'remove-snapshot' and args.snapshot:
        mongodb.storage().force_remove_snapshot(args.snapshot)    
    elif args.command == 'remove-app-installs' and args.snapshot:
        mongodb.storage().remove_app_installs(args.snapshot)
    elif args.command == 'get-stats':
        stats = Stats(mongodb.stats_queries())
        print(stats.get_all(args.data_history))
    elif args.command == 'get-app-counts-csv':
        stats = Stats(mongodb.stats_queries())
        print(stats.get_app_counts_csv())
    elif args.command == 'get-users-by-app':
        stats = Stats(mongodb.stats_queries())
        print(stats.get_users_by_app(args.app, args.snapshot))
    elif args.command == 'get-historic-app-counts-csv':
        stats = Stats(mongodb.stats_queries())
        print(stats.get_all_apps_time_series())


if __name__ == "__main__":
    main()
