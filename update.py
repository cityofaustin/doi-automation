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
    # add arg to command line execution eg; 'python update.py "Austin Resource Recovery"'
    # parser.add_argument('departments', metavar='D', type=str, nargs=1,
    #                     help='Optional name of CoA department for update')
    # department = parser.parse_args()
    # department_df = pd.read_json(departments_json)
    # dept_list = department_df['Department'].tolist()
    #
    # # exit if department arg incorrect
    # if department is not None and department not in dept_list:
    #     print('Unknown department, please use one of the following:\n{}'.format("\n".join(str(x) for x in dept_list)))
    #     return

    old_assets_df = pd.read_json(socrata_assets_json)
    old_assets = old_assets_df.to_dict('records')
    assets = gather_socrata_assets()

    # # update department in param
    # if department is not None:
    #     dept_assets = (next(item for item in assets if item["department"] == department))
    #     assets_df = pd.DataFrame(dept_assets)
    # update all depts in doi_assets table
    # else:
    #     assets_df = pd.DataFrame(assets)

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
    atd_assets = []
    for a in assets:
        if a['department'] == 'Austin Transportation':
            atd_assets.append(a)
    assets_df = pd.DataFrame(atd_assets)
    temp_table = load_temp_table(atd_assets)
    # adds_df = find_adds(temp_table)
    # diff_df = find_changes(temp_table)
    # doi = gather_doi_assets()

    old_assets_df = pd.read_json(socrata_assets_json)
    old_assets = old_assets_df.to_dict('records')

    for index, row in assets_df.iterrows():
        update_static_table(assets, socrata_4x4=row['socrata_4x4'])
        success = publish_doi(socrata_4x4=row['socrata_4x4'], temp_table=temp_table)
        update_static_table(assets, socrata_4x4=row['socrata_4x4'])
        if success is False:
            # revert table if request fails
            print('create Failed')
            update_static_table(old_assets, socrata_4x4=row['socrata_4x4'])
        elif success is True:
            print('created {}'.format(row['socrata_4x4']))
    # print(atd_assets)


if __name__ == "__main__":
    assets = gather_socrata_assets()
    for asset in assets:
        print(asset['name'])

    # old_assets_df = pd.read_json(socrata_assets_json)
    # old_assets = old_assets_df.to_dict('records')
    # assets = gather_socrata_assets()
    # atd_assets = []
    # for a in assets:
    #     if a['department'] == 'Austin Resource Recovery':
    #         atd_assets.append(a)
    # assets_df = pd.DataFrame(atd_assets)
    # temp_table = load_temp_table(atd_assets)
    # # diff_df = find_changes(assets_df)
    #
    # for index, row in assets_df.iterrows():
    #     # update static table request pulls from
    #     update_static_table(assets, socrata_4x4=row['socrata_4x4'])
    #     try:
    #         success = update_doi(row['socrata_4x4'], temp_table=temp_table, draft=False)
    #         if success is False:
    #             # revert table if request fails
    #             print('Update Failed')
    #             update_static_table(old_assets, socrata_4x4=row['socrata_4x4'])
    #         elif success is True:
    #             print('Updated {}'.format(row['socrata_4x4']))
    #     except ValueError:
    #         # revert table if doi does not exist in table
    #         print('Socrata asset {} does not exist in DOI table'.format(row['name']))
    #         update_static_table(old_assets, socrata_4x4=row['socrata_4x4'])

