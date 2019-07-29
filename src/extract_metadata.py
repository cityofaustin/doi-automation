import requests
import os
import json
import datetime

import pandas as pd

assets = []
count = 0
fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data//socrata_assets.json')
socrata_assets_json = os.path.abspath(os.path.realpath(filename))
filename = os.path.join(fileDir, 'data//doi_assets.json')
doi_assets_json = os.path.abspath(os.path.realpath(filename))


def gather_socrata_assets():
    """Gathers assets from socrata for metadata generation and returns a list of dictionaries. Dictionaries keys are
    field names as required by DataCite and values are from Socrata"""
    global count
    global assets
    # limits query to 2000 results
    url = 'http://api.us.socrata.com/api/catalog/v1?limit=2000&domains=data.austintexas.gov'
    r = requests.get(url).json()

    for item in r['results']:
        try:
            # retrieve department name by looking for field with Department in name. Domain metadata field order varies
            for dictionary in item['classification']['domain_metadata']:
                for key in dictionary.keys():
                    if 'Department' in dictionary['key']:
                        department = dictionary['value']
            # assemble dictionary and append to asset list
            if item['resource']['type'] == 'dataset':
                assets.append({'socrata_4x4': item['resource']['id'],
                               'name': item['resource']['name'],
                               'department': department,
                               'type': item['resource']['type'],
                               'year': item['resource']['createdAt'].split('-')[0],
                               'permalink': item['permalink'],
                               'desc': item['resource']['description']})

        except IndexError:
            # count assets that do not have fields above by throwing an IndexError
            count += 1
    return assets


def gather_doi_assets():
    """Query DataCite for client DOIs and compare with socrata asset json"""

    url = 'https://api.datacite.org/clients/austintx.atxdr/dois'
    r = requests.get(url)
    results = json.loads(r.content)
    page_count = results['meta']['totalPages']
    count = 0

    doi_assets = pd.read_json(doi_assets_json)
    doi_list = list(doi_assets['doi'])

    assets = gather_socrata_assets()

    datacite_user = os.environ['datacite_user']
    datacite_pass = os.environ['datacite_pass']

    while count < page_count:
        count += 1
        url = 'https://api.datacite.org/clients/austintx.atxdr/dois?page[number]={}'.format(count)
        r = requests.get(url)
        response = json.loads(r.content)
        for asset in response['data']:
            doi = asset['id']
            if doi in doi_list:
                print('{} already accounted for'.format(doi))
            else:
                print('{} not accounted for'.format(doi))
                url = 'https://api.datacite.org/works/{}'.format(doi)
                r = requests.get(url, auth=(datacite_user, datacite_pass))
                response = json.loads(r.content)
                xml = response['data']['attributes']['xml']
                title = response['data']['attributes']['title']
                try:
                    socrata_asset = next(item for item in assets if item["name"] == title)
                    socrata_4x4 = socrata_asset['socrata_4x4']
                    doi_assets = doi_assets.append({'socrata_4x4': socrata_4x4,
                                                    'doi': doi,
                                                    'metadata_xml': xml,
                                                    'doi_prefix': doi.split('/')[-1].split('.')[0],
                                                    'doi_suffix': doi.split('/')[-1].split('.')[1],
                                                    'department': socrata_asset['department'],
                                                    'created_at': str(datetime.datetime.now())}, ignore_index=True)
                    print(title, socrata_4x4)
                    doi_assets.to_json(doi_assets_json)
                except StopIteration:
                    print('{} title not found'.format(doi))


def load_temp_table(temp_assets):
    """Load list of dictionaries into temp table for diff"""
    # use env variables for credentials

    temp_df = pd.DataFrame(temp_assets)

    return temp_df


def update_static_table(assets, socrata_4x4=None):
    """Update static socrata assets table. Supply socrata_4x4 to only update that record."""

    if socrata_4x4 is not None:
        # print('recording '+ socrata_4x4)
        # Find socrata_4x4 metadata in asset list.
        record = (next(item for item in assets if item["socrata_4x4"] == socrata_4x4), '4x4 does not exist in static')
        # read json, update, and save
        static_table = pd.read_json(socrata_assets_json)
        static_table.loc[static_table['socrata_4x4'] == socrata_4x4, 'desc'] = record[0]['desc']
        static_table.loc[static_table['socrata_4x4'] == socrata_4x4, 'name'] = record[0]['name']
        static_table.to_json(socrata_assets_json)
        return
    else:
        print('updating all')
        # update json with all latest records
        all_new_assets = pd.DataFrame(assets)
        all_new_assets.to_json(socrata_assets_json)
        return


if __name__ == "__main__":
    gather_socrata_assets()

