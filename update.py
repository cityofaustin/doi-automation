"""Compares current socrata assets as temporary table with last scan's (static table). Assets that are detected to have
been changed are updated in DataCite and the static table is updated. Assets that have been added are created"""
# import argparse
import os
# import json


import pandas as pd
from extract_metadata import *
from diff_assets import *
from update_doi import update_doi
from publish_doi import *

from extract_metadata import gather_doi_assets


fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data//socrata_assets.json')
socrata_assets_json = os.path.abspath(os.path.realpath(filename))

filename = os.path.join(fileDir, 'data//departments.json')
departments_json = os.path.abspath(os.path.realpath(filename))

# parser = argparse.ArgumentParser(description='Update DataCite assets from socrata.')


def main():

    old_assets_df = pd.read_json(socrata_assets_json)
    old_assets = old_assets_df.to_dict('records')
    assets = gather_socrata_assets()

    assets_df = pd.DataFrame(assets)
    diff_df = find_changes(assets_df)

    # exit if there are no differences
    if len(diff_df) == 0:
        print('No Updates Detected...')
        return

    for index, row in diff_df.iterrows():
        # update static table request pulls from
        update_static_table(assets, socrata_4x4=row['socrata_4x4'])
        try:
            success = update_doi(row['socrata_4x4'])
            if success is False:
                # revert table if request fails
                print('Update Failed')
                update_static_table(old_assets, socrata_4x4=row['socrata_4x4'])
            elif success is True:
                print('Updated {}'.format(row['socrata_4x4']))
        except ValueError:
            # revert table if doi does not exist in table
            print('Socrata asset {} does not exist in DOI table'.format(row['name']))
            update_static_table(old_assets, socrata_4x4=row['socrata_4x4'])


def create_new():
    # creating newly added DOIs
    assets = gather_socrata_assets()
    assets[:] = [d for d in assets if d.get('department') != "Emergency Medical Services"]
    assets[:] = [d for d in assets if d.get('department') != "Austin Transportation"]
    assets[:] = [d for d in assets if d.get('department') != "Austin Resource Recovery"]
    assets_df = pd.DataFrame(assets)
    temp_table = load_temp_table(assets)
    # doi = gather_doi_assets()

    old_assets_df = pd.read_json(socrata_assets_json)
    old_assets = old_assets_df.to_dict('records')
    doi_assets = pd.read_json(doi_assets_json)

    for index, row in assets_df.iterrows():
        if row['socrata_4x4'] in list(doi_assets['socrata_4x4']):
            continue
        update_static_table(assets, socrata_4x4=row['socrata_4x4'])
        success = publish_doi(socrata_4x4=row['socrata_4x4'], temp_table=temp_table, draft=False)
        update_static_table(assets, socrata_4x4=row['socrata_4x4'])
        if success is False:
            # revert table if request fails
            print('create Failed')
            update_static_table(old_assets, socrata_4x4=row['socrata_4x4'])
        elif success is True:
            print('created {}'.format(row['socrata_4x4']))
    # print(atd_assets)


if __name__ == "__main__":
    create_new()
