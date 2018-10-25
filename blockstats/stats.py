import json
from collections import defaultdict
from datetime import datetime

class Stats:

    def __init__(self, stats_queries):
        self._stats_queries = stats_queries

    def get_all(self, data_history_file):

        historical_stats = None
        if data_history_file:
            with open(data_history_file, 'r') as f:
                historical_stats = json.load(f)

        q = self._stats_queries
        totals = q.get_total_address_counts()
        domain_counts = q.get_domain_counts()
        subdomains_counts = q.get_subdomain_counts()
        person_counts = q.get_person_counts()
        total_installs = q.get_total_installs_time_series()
        localhost_installs = q.get_localhost_installs_time_series()

        values_by_date = defaultdict(list)
        merge(values_by_date, 'total_addresses', totals)
        merge(values_by_date, 'domains', domain_counts)
        merge(values_by_date, 'subdomains', subdomains_counts)
        merge(values_by_date, 'persons', person_counts)
        merge(values_by_date, 'total_installs', total_installs)
        merge(values_by_date, 'localhost_installs', localhost_installs)

        domains_data = []
        for date, values in values_by_date.items():
            domains_data.append({
                'date': date,
                'values': values
            })

        if historical_stats:
            domains_data.extend(historical_stats['domainsData'])

        domains_data.sort(key=lambda d: datetime.strptime(d['date'], '%Y-%m-%d'))

        latest_snapshot = q.get_latest_snapshot()
        top10_apps = q.get_app_counts(latest_snapshot['_id'])[0:10]
        top10_app_names = [app['name'] for app in top10_apps]

        apps_data = q.get_app_time_series(top10_app_names)

        if historical_stats:
            def leave_only_top10(item):
                item['values'] = [app for app in item['values'] if app['name'] in top10_app_names]
            for item in historical_stats['appsData']:
                leave_only_top10(item)

        apps_data.extend(historical_stats['appsData'])
        apps_data.sort(key=lambda d: datetime.strptime(d['date'], '%Y-%m-%d'))

        all_stats = {'domainsData': domains_data, 'appsData': apps_data}

        return json.dumps(all_stats, indent=3)

def merge(values_dict, name, values_arr):
    for d in values_arr:
        date = d['date']
        values_dict[date].append({
            'name': name,
            'count': d['value']
        })
