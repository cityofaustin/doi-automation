import requests
import os

import pandas as pd

assets = []
count = 0
fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data\\socrata_assets.json')
socrata_assets_json = os.path.abspath(os.path.realpath(filename))


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


#TODO: figure out how to get ALL (including draft) datacite assets
def gather_doi_assets():
    pass


def load_temp_table(temp_assets):
    """Load list of dictionaries into temp table in database for diff"""
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
    pass

