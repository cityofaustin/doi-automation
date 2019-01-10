import os


import pandas as pd

fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data\\socrata_assets.json')
socrata_assets_json = os.path.abspath(os.path.realpath(filename))


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
    static_table = pd.read_json(socrata_assets_json)
    # find added assets
    add_join = temp_table.merge(static_table, on=['socrata_4x4'], how='left', indicator=True)
    print(add_join.head())
    adds_df = add_join[add_join['_merge'] == 'left_only']
    return adds_df


def find_deletes(temp_table):
    pass


if __name__ == "__main__":
    from extract_metadata import *
    assets = gather_socrata_assets()
    #temp_table_df = load_temp_table(assets)
    #test = find_adds(temp_table_df)
    print(socrata_assets_json)