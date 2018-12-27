import requests
import os

import pandas as pd

assets = []
count = 0

fileDir = os.path.dirname(os.path.realpath('__file__'))

filename = os.path.join(fileDir, '../data/socrata_assets.json')
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


def gather_doi_assets():
    pass


def load_temp_table(temp_assets):
    """Load list of dictionaries into temp table in database for diff"""
    # use env variables for credentials

    temp_df = pd.DataFrame(temp_assets)

    return temp_df


def update_static_table(assets):

    static_update_table = pd.DataFrame(assets)
    static_update_table.to_json(socrata_assets_json)
    # return perm_table


def diff_temp_table(temp_table):
    """Perform diff of newly retrieved socrata assets in temp table against static table."""
    static_table = pd.read_json(socrata_assets_json)

    # find name or description changes
    change_join = temp_table.merge(static_table, on=['desc', 'name'])
    changed = temp_table[(~temp_table.desc.isin(change_join.desc))]

    # TODO not working. Socrata wonkyness making testing difficult
    # find added assets
    add_join = temp_table.merge(static_table, on=['socrata_4x4'], how='left', indicator=True)
    print(add_join.head())
    adds = add_join[add_join['_merge'] == 'left_only']

    # find deleted assets not sure if this will be useful because DataCite does not allow deletion of non-draft state DOIs
    del_join = temp_table.merge(static_table, on=['socrata_4x4'], how='right', indicator=True)
    deletes = del_join[del_join['_merge'] == 'right_only']

    return changed, adds


if __name__ == "__main__":
    print(count)
    assets = gather_socrata_assets()
    print(len(assets))
    changes, adds = diff_temp_table(load_temp_table(assets))
    print(changes.head())
    print(adds.head())

