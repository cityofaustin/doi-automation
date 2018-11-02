import requests
import os


import psycopg2

assets = []
count = 0


def gather_assets():
    """Gathers assets from socrata for metadata generation and returns a list of dictionaries. Dictionaries keys are
    field names as required by DataCite and values are from Socrata"""
    global count
    global assets
    # limits query to 2000 results
    url = 'http://api.us.socrata.com/api/catalog/v1?limit=2000&domains=data.austintexas.gov'
    r = requests.get(url).json()

    for item in r['results']:
        try:
            # retrieve department name by looking for field with Department in name. Domain metadata field order varies
            for dictionary in item['classification']['domain_metadata']:
                for key in dictionary.keys():
                    if 'Department' in dictionary['key']:
                        department = dictionary['value']
            # assemble dictionary and append to asset list
            if item['resource']['type'] == 'dataset':
                assets.append({'socrata_4x4': item['resource']['id'],
                               'name': item['resource']['name'],
                               'department': department,
                               'type': item['resource']['type'],
                               'year': item['resource']['createdAt'].split('-')[0],
                               'permalink': item['permalink'],
                               'desc': item['resource']['description']})

        except IndexError:
            # count assets that do not have fields above by throwing an IndexError
            count += 1
    return assets


def load_temp_table(temp_assets):
    """Load list of dictionaries into temp table in database for diff"""
    # use env variables for credentials
    conn = psycopg2.connect(host="localhost", database="citation-station",
                            user=os.environ['postgres_user'],
                            password=os.environ['postgres_pass'])
    cur = conn.cursor()
    # create temp table
    tbl_statement = """CREATE TEMPORARY TABLE temp_socrata_asset_metadata(
                        PRIMARY KEY (socrata_4x4)) 
                        INHERITS (socrata_asset_metadata)"""
    cur.execute(tbl_statement)

    # insert list of dictionaries into temp table
    cur.executemany("""INSERT INTO temp_socrata_asset_metadata(socrata_4x4,name,department,type,year,permalink,"desc")
                          VALUES (%(socrata_4x4)s, %(name)s, %(department)s, %(type)s, %(year)s, %(permalink)s, %(desc)s)""", temp_assets)
    conn.commit()
    cur.close()
    return conn


def diff_temp_table(conn):
    """Perform diff of newly retrieved socrata assets against in temp table against static table."""
    cur = conn.cursor()
    diff_query = """(   SELECT * FROM temp_socrata_asset_metadata
                        EXCEPT
                        SELECT * FROM socrata_asset_metadata)  
                    UNION ALL
                    (   SELECT * FROM socrata_asset_metadata
                        EXCEPT
                        SELECT * FROM temp_socrata_asset_metadata) """
    cur.execute(diff_query)
    diff_list = cur.fetchall()
    cur.close()
    conn.close()
    return diff_list


if __name__ == "__main__":

    result_assets = gather_assets()
    connection = load_temp_table(result_assets)
    diff = diff_temp_table(connection)

    print(len(result_assets))
    for d in diff:
        print(d)
    for asset in result_assets:
        print(asset)
