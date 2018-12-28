import os
import requests
import datetime
import json


import pandas as pd

from extract_metadata import gather_socrata_assets
from extract_metadata import update_static_table
from publish_doi import assemble_payload

fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data\\doi_assets.json')
assets_filename = os.path.abspath(os.path.realpath(filename))


def update_doi(socrata_4x4, draft=True):
    """Updates existing DOI's metadata if diff detects a change"""

    url = 'https://api.datacite.org/dois/'
    datacite_user = os.environ['datacite_user']
    datacite_pass = os.environ['datacite_pass']

    doi_assets = pd.read_json(assets_filename)

    payload, doi, xml, metadata = assemble_payload(socrata_4x4, draft, update=True)

    r = requests.put('{}{}'.format(url, doi), json=payload, auth=(datacite_user, datacite_pass))
    if r.content[2:8] == 'errors':
        print('DataCite error \n')
        print(r.content.json())
        return
    else:
        # update doi_assets
        print(r.content)
        doi_assets.loc[doi_assets['socrata_4x4'] == socrata_4x4, 'metadata_xml'] = xml
        doi_assets.loc[doi_assets['socrata_4x4'] == socrata_4x4, 'last_updated'] = str(datetime.datetime.now())
        doi_assets.to_json(filename)


if __name__ == "__main__":
    pass


