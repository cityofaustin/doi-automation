"""Compares current socrata assets as temporary table with last scan's (static table). Assets that are detected to have
been changed are updated in DataCite and the static table is updated."""
# import sys


import pandas as pd
from extract_metadata import gather_socrata_assets
from extract_metadata import update_static_table
from diff_assets import find_changes
from update_doi import update_doi


def main(department=None):
    assets = gather_socrata_assets()
    assets_df = pd.DataFrame(assets)

    diff_df = find_changes(assets_df)
    if len(diff_df) == 0:
        print('No Updates Detected...')

    for index, row in diff_df.iterrows():
        print('Updating: ' + row['socrata_4x4'], row['name'])
        update_static_table(assets, socrata_4x4=row['socrata_4x4'])
        print('DataCite Response:')
        update_doi(row['socrata_4x4'])
        print("Updated: \n" +  row['socrata_4x4'], row['name']+"\n")


if __name__ == "__main__":
    main()