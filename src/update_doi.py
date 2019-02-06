import os
import requests
import datetime
import json
# TODO add logging
# import logging


import pandas as pd

# from extract_metadata import gather_socrata_assets
# from extract_metadata import update_static_table
from extract_metadata import gather_doi_assets
from publish_doi import assemble_payload

fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data//doi_assets.json')
assets_filename = os.path.abspath(os.path.realpath(filename))


def update_doi(socrata_4x4, temp_table, draft=True):
    """Updates existing DOI's metadata"""

    url = 'https://api.datacite.org/dois/'
    datacite_user = os.environ['datacite_user']
    datacite_pass = os.environ['datacite_pass']
    print('Updating: ' + socrata_4x4)

    doi_assets = pd.read_json(assets_filename)

    payload, doi, xml, metadata = assemble_payload(socrata_4x4, temp_table, draft, update=True)

    r = requests.put('{}{}'.format(url, doi), json=payload, auth=(datacite_user, datacite_pass))
    response_dict = json.loads(r.content)
    print('DataCite Response:')
    print(response_dict)
    if 'data' in response_dict:
        # TODO add git commit of file to push to github repo
        # TODO linking lines to github?
        # if successful update doi table
        doi_assets.loc[doi_assets['socrata_4x4'] == socrata_4x4, 'metadata_xml'] = xml
        doi_assets.loc[doi_assets['socrata_4x4'] == socrata_4x4, 'last_updated'] = str(datetime.datetime.now())
        doi_assets.to_json(filename)
        return True
    else:
        print('Error, could not update: ' + socrata_4x4)
        return False


if __name__ == "__main__":
    pass


