import requests
import psycopg2
from psycopg2.extensions import AsIs
# import base64
import os
# import json
# import xmlrpc.client

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
            count +=1
    return assets


def load_temp_table(temp_assets):
    """Load list of dictionaries into temp table in database for diff"""
    # use env variables for credentials
    conn = psycopg2.connect(host="localhost", database="citation-station",
                            user=os.environ['postgres_user'],
                            password=os.environ['postgres_pass'])
    cur = conn.cursor()

    tbl_statement = """CREATE TEMPORARY TABLE temp_socrata_asset_metadata(
                 ) INHERITS (socrata_asset_metadata)"""
    cur.execute(tbl_statement)

    cur.executemany("""INSERT INTO temp_socrata_asset_metadata(socrata_4x4,name,department,type,year,permalink,"desc")
                          VALUES (%(socrata_4x4)s, %(name)s, %(department)s, %(type)s, %(year)s, %(permalink)s, %(desc)s)""", temp_assets)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    result_assets = gather_assets()
    load_temp_table(result_assets)
    print(len(result_assets))
    print("{} assets do not contain metadata".format(count))
    depts = []
    for asset in result_assets:
        print(asset)
