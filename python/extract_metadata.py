import requests
# import os
# import json
# import xmlrpc.client

datasets = []
count = 0


def gather_all_assets():
    """Gathers datasets from socrata for metadata generation and compiles a list."""
    global count
    global datasets
    # limits query to 2000 results
    url = 'http://api.us.socrata.com/api/catalog/v1?limit=2000&domains=data.austintexas.gov'
    r = requests.get(url).json()

    for item in r['results']:
        try:
            datasets.append({item['resource']['id']: (item['resource']['name'], # name
                                                      item['classification']['domain_metadata'][0]['value'], # department
                                                      item['resource']['type'], # type
                                                      item['resource']['createdAt'].split('-')[0], # publication year
                                                      item['permalink'], # permalink
                                                      item['resource']['description'] # description
                                                     )
                            }
                           )
        except IndexError:
            count +=1
    return datasets


if __name__ == "__main__":
    assets = gather_all_assets()
    print(assets[50])