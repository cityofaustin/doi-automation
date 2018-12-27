import os
import requests
import datetime


import pandas as pd

from extract_metadata import gather_socrata_assets
from publish_doi import assemble_payload

fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, '../data/doi_assets.json')
assets_filename = os.path.abspath(os.path.realpath(filename))


def update_doi(socrata_4x4):
    """Updates existing DOI's metadata if diff detects a change"""

    doi_assets = pd.read_json(filename)
    url = 'https://api.datacite.org/dois/'
    datacite_user = os.environ['datacite_user']
    datacite_pass = os.environ['datacite_pass']

    payload, doi, xml, metadata = assemble_payload(socrata_4x4, update=True)

    r = requests.put('{}{}'.format(url, doi), auth=(datacite_user, datacite_pass))
    if r.content == '{"errors":[{"status":"400","title":"You need to provide a payload following the JSONAPI spec"}]}':
        print('DataCite payload error')
        print(r.content)
        return
    else:
        # update doi_assets
        doi_assets.loc[doi_assets['socrata_4x4'] == socrata_4x4, 'metadata_xml'] = xml
        doi_assets.loc[doi_assets['socrata_4x4'] == socrata_4x4, 'last_updated'] = str(datetime.datetime.now())
        doi_assets.to_json(filename)


if __name__ == "__main__":

    result_assets = gather_socrata_assets()
    for r in result_assets:
        if r['department'] == "Austin Resource Recovery":
            formatted = ([list(r.values())])
            print(r)
            update_doi(r['socrata_4x4'])


