import requests
import psycopg2
# import base64
import os
# import json
# import xmlrpc.client

assets = []
count = 0


def gather_assets():
    """Gathers assets from socrata for metadata generation and compiles a dictionary of tuples."""
    global count
    global assets
    # limits query to 2000 results
    url = 'http://api.us.socrata.com/api/catalog/v1?limit=2000&domains=data.austintexas.gov'
    r = requests.get(url).json()

    for item in r['results']:
        try:
            if item['resource']['type'] == 'dataset':
                assets.append({item['resource']['id']: (item['resource']['name'],  # name
                                                        item['classification']['domain_metadata'][0]['value'],  # department
                                                        item['resource']['type'],  # type
                                                        item['resource']['createdAt'].split('-')[0],  # publication year
                                                        item['permalink'],  # permalink
                                                        item['resource']['description']  # description
                                                        )
                                }
                               )
        except IndexError:
            count +=1
    return assets


def load_db():
    conn = psycopg2.connect(host="localhost", database="citation-station", user=os.environ['postgres_user'],
                            password=os.environ['postgres_pass'])


if __name__ == "__main__":
    result_assets = gather_assets()
    print(len(result_assets))
    print("{} assets do not contain metadata".format(count))
    for asset in result_assets:
        print(asset)