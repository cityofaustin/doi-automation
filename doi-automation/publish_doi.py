import psycopg2
# import requests
import os


def assemble_payload(socrata_4x4):
    """Assembles json for doi creation post. Identifies dept prefix and doi suffix."""
    conn = psycopg2.connect(host="localhost", database="citation-station",
                            user=os.environ['postgres_user'],
                            password=os.environ['postgres_pass'])
    cur = conn.cursor()

    # fetch metadata
    select_metadata = """SELECT socrata_4x4, name, department, type, year, permalink, "desc" 
                            FROM
                         socrata_asset_metadata 
                            WHERE socrata_4x4 = '{}'""".format(socrata_4x4)
    cur.execute(select_metadata)
    metadata = cur.fetchall()

    # figure out which department prefix to use
    select_dept_prefix = """SELECT prefix FROM dept_prefixes WHERE name = '{}'""".format(metadata[0][2])
    cur.execute(select_dept_prefix)
    dept_prefix = cur.fetchall()

    # figure out next doi suffix to use
    select_department_dois = """SELECT * FROM doi_assets WHERE department = '{}'""".format(metadata[0][2])
    cur.execute(select_department_dois)
    department_dois = cur.fetchall()

    suffix_list = []
    for doi in department_dois:
        suffix_list.append(int(doi[6]))

    doi_suffix = str((max(suffix_list)+1)).zfill(6)

    payload = {'data':
                {'type':
                    'dois',
                    'attributes':
                        {'doi':
                            '10.26000/{}.{}'.format(dept_prefix[0][0], doi_suffix)},
                    'relationships':
                        {'client':
                            {'data':
                                {'type':
                                    'clients',
                                    'id': 'AUSTINTX.ATXDR'}
                             }
                         }
                 }
               }
    return payload


if __name__ == "__main__":
    output = assemble_payload('jhra-82n2')
    print(output)