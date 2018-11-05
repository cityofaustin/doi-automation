import os
import requests

import psycopg2


def assemble_payload(conn, socrata_4x4):
    """Assembles json for doi creation post. Identifies dept prefix and doi suffix."""
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
    # add 1 to highest suffix value and use zfill to pad w/zeros to 6 chars
    doi_suffix = str((max(suffix_list)+1)).zfill(6)
    identifier = '10.26000/{}.{}'.format(dept_prefix[0][0], doi_suffix)

    # now that we have the identifier we can assemble the xml string
    xml_encoded = assemble_xml(metadata, identifier)

    payload = {'data':
                {'type':
                    'dois',
                    'attributes':
                        {'doi':
                            '{}'.format(identifier),
                         'url':
                            '{}'.format(metadata[0][5]),
                         'xml':
                            '{}'.format(xml_encoded),
                         },

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


def assemble_xml(metadata, doi_identifier):
    """Uses draft XML from datacite and edits required nodes to assemble XML."""
    import base64
    import xml.etree.ElementTree as ET
    datacite_example = '{}\\xml\\datacite-example.xml'.format((os.path.dirname(os.path.realpath(__file__))))

    # get directory to find xml example
    tree = ET.parse(datacite_example)
    root = tree.getroot()
    resource_id = root[0].tag.split('}')[0].lstrip('{')
    # set doi identifier
    for node in root.iter('{http://datacite.org/schema/kernel-3}identifier'):
        node.text = str(doi_identifier)
    # set title
    for node in root.iter('{http://datacite.org/schema/kernel-3}title'):
        node.text = metadata[0][1]
    # set pub year
    for node in root.iter('{http://datacite.org/schema/kernel-3}publicationYear'):
        node.text = str(metadata[0][4])
    # set creator/dept
    for node in root.iter('{http://datacite.org/schema/kernel-3}creator'):
        node.text = metadata[0][2]
    # set description
    for node in root.iter('{http://datacite.org/schema/kernel-3}description'):
        node.text = metadata[0][6]
    # creating a temporary intermediate xml. Not sure if this is necessary.
    # We can just use the base64 string for loading into postgres.
    output_xml = ('{}\\xml\\datacite-test.xml'.format((os.path.dirname(os.path.realpath(__file__)))))
    # create temp output
    tree.write(output_xml)

    tree = ET.parse(output_xml)
    root = tree.getroot()
    os.remove(output_xml)
    # for node in root.iter():
    #     print(node.tag)
    #     print(node.text)

    xmlstr = ET.tostring(root, encoding='utf-8', method='xml')
    xml_encoded = base64.b64encode(xmlstr)

    return xml_encoded.decode('utf-8')


def insert_xml(conn):
    pass


def publish_doi(payload, draft=True):
    """Publishes DOI, encodes XML into base64 and inserts DOI record into postgres"""
    import requests

    datacite_user = os.environ['datacite_user']
    datacite_pass = os.environ['datacite_pass']
    url = 'https://api.datacite.org/dois'
    r = requests.post(url, json=payload, auth=(datacite_user, datacite_pass))
    print(r.content)


if __name__ == "__main__":
    conn = psycopg2.connect(host="localhost", database="citation-station",
                            user=os.environ['postgres_user'],
                            password=os.environ['postgres_pass'])
    payload= assemble_payload(conn, 'jhra-82n2')
    publish_doi(payload)
