import requests


import pandas as pd

assets = []
count = 0


def gather_assets():
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


def load_temp_table(temp_assets):
    """Load list of dictionaries into temp table in database for diff"""
    # use env variables for credentials

    temp_df = pd.DataFrame(temp_assets)

    return temp_df


def update_perm_table(assets):

    perm_update_table = pd.DataFrame(assets)
    perm_update_table.to_json('data\\socrata_assets.json')
    # return perm_table


def diff_temp_table(temp_table):
    """Perform diff of newly retrieved socrata assets against in temp table against static table."""
    perm_table = pd.read_json('data\\socrata_assets.json')

    # find name or description changes
    common = temp_table.merge(perm_table, on=['desc', 'name'])
    changed = temp_table[(~temp_table.desc.isin(common.desc))]
    return changed


if __name__ == "__main__":

    result_assets = gather_assets()
    temp_table = load_temp_table(result_assets)
    perm_table = update_perm_table(result_assets)
    diff = diff_temp_table(temp_table)

    print(diff['socrata_4x4'])
