import os


import pandas as pd


socrata_assets_json = 'data\\socrata_assets.json'


def find_changes(temp_table):
    """Perform diff of newly retrieved socrata assets in temp table against static table."""
    static_table = pd.read_json(socrata_assets_json)

    # find name or description changes
    change_join = temp_table.merge(static_table, on=['desc', 'name'])
    changed = temp_table[(~temp_table.desc.isin(change_join.desc))]

    # find deleted assets not sure if this will be useful because DataCite does not allow deletion of non-draft state DOIs
    del_join = temp_table.merge(static_table, on=['socrata_4x4'], how='right', indicator=True)
    deletes = del_join[del_join['_merge'] == 'right_only']

    return changed


def find_adds(temp_table):
    # TODO not working. Socrata wonkyness making testing difficult
    static_table = pd.read_json(socrata_assets_json)
    # find added assets
    add_join = temp_table.merge(static_table, on=['socrata_4x4'], how='left', indicator=True)
    print(add_join.head())
    adds = add_join[add_join['_merge'] == 'left_only']
    pass
