"""Compares current socrata assets as temporary table with last scan's (static table). Assets that are detected to have
been changed are updated in DataCite and the static table is updated."""
# import sys
import os
import json


import pandas as pd
from extract_metadata import gather_socrata_assets
from extract_metadata import update_static_table
from diff_assets import find_changes
from update_doi import update_doi


fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data\\socrata_assets.json')
socrata_assets_json = os.path.abspath(os.path.realpath(filename))


def main(department=None):

    old_assets_df = pd.read_json(socrata_assets_json)
    old_assets = old_assets_df.to_dict('records')
    assets = gather_socrata_assets()
    assets_df = pd.DataFrame(assets)

    diff_df = find_changes(assets_df)

    if len(diff_df) == 0:
        print('No Updates Detected...')

    for index, row in diff_df.iterrows():
        update_static_table(assets, socrata_4x4=row['socrata_4x4'])
        success = update_doi(row['socrata_4x4'])
        if success is False:
            # revert table if fail
            print('Update Failed')
            update_static_table(old_assets, socrata_4x4=row['socrata_4x4'])
        elif success is True:
            print('Updated {}'.format(row['socrata_4x4']))



if __name__ == "__main__":
    main()