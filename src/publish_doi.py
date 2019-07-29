import os
import datetime


import pandas as pd


fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data//doi_assets.json')
doi_assets_json = os.path.abspath(os.path.realpath(filename))

filename = os.path.join(fileDir, 'data//socrata_assets.json')
socrata_assets_json = os.path.abspath(os.path.realpath(filename))

filename = os.path.join(fileDir, 'data//departments.json')
departments_json = os.path.abspath(os.path.realpath(filename))

filename = os.path.join(fileDir, 'data//datacite-example.xml')
datacite_xml = os.path.abspath(os.path.realpath(filename))


def assemble_payload(socrata_4x4, temp_table=None, draft=True, update=False):
    """Assembles json for doi creation post & update put. Identifies dept prefix and doi suffix."""

    # Gather static data
    socrata_assets = pd.read_json(socrata_assets_json)
    departments = pd.read_json(departments_json)
    doi_assets = pd.read_json(doi_assets_json)

    if temp_table is not None:
        # fetch metadata if not new
        metadata = temp_table.loc[temp_table['socrata_4x4'] == socrata_4x4]
    else:
        # fetch metdata if new
        metadata = socrata_assets.loc[socrata_assets['socrata_4x4'] == socrata_4x4]

    # figure out which department prefix to use
    dept_prefix = departments.loc[departments['Department'] == metadata['department'].item()]

    if update is False:
        # figure out next doi suffix to use if new
        department_dois = doi_assets.loc[doi_assets['department'] == metadata['department'].item()]
        if len(department_dois) != 0:
            # add 1 to highest suffix value and use zfill to pad w/zeros to 6 chars
            doi_suffix = str(int(department_dois['doi_suffix'].max()+1)).zfill(6)
        else:
            # new doi for dept, start at 000001
            doi_suffix = str(1).zfill(6)
        # zfill dept prefix to 3 chars
        try:
            doi = '10.26000/{}.{}'.format(str(dept_prefix.index[0]).zfill(3), doi_suffix)
        except IndexError:
            # unknown or mispelled department names, eg "Parks and Recreaction"
            doi = '10.26000/{}.{}'.format(str('43'.zfill(3)), doi_suffix)
    else:
        # find existing doi for update
        doi = doi_assets.loc[doi_assets['socrata_4x4'] == socrata_4x4]['doi'].item()

    # now that we have the identifier we can assemble the xml string
    xml_encoded = assemble_xml(metadata, doi)

    payload = {'data':
                {'type':
                    'dois',
                    'attributes':
                        {'doi':
                            '{}'.format(doi),
                         'url':
                            '{}'.format(metadata['permalink'].item()),
                         'xml':
                            '{}'.format(xml_encoded)
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

    # important, because datacite does not let you permanently delete published DOIs.
    if draft is True:
        pass
    else:
        payload['data']['attributes'].update({'event': 'publish'})

    return payload, doi, xml_encoded, metadata


def assemble_xml(metadata, doi):
    """Uses draft XML from datacite and edits required nodes to assemble XML."""
    import base64
    import xml.etree.ElementTree as ET

    # get directory to find xml example
    tree = ET.parse(datacite_xml)
    root = tree.getroot()
    # resource_id = root[0].tag.split('}')[0].lstrip('{')

    # set doi identifier
    for node in root.iter('{http://datacite.org/schema/kernel-3}identifier'):
        node.text = str(doi)
    # set title
    for node in root.iter('{http://datacite.org/schema/kernel-3}title'):
        node.text = metadata['name'].item()
    # set pub year
    for node in root.iter('{http://datacite.org/schema/kernel-3}publicationYear'):
        node.text = str(metadata['year'].item())
    # trying to fix name display for citations
    if metadata['department'].item().split()[0] == 'Austin' and metadata['department'].item().split()[-1] != 'Department':
        fixed_name = metadata['department'].item().split(' ', 1)[1] + ' Department'
        for node in root.iter('{http://datacite.org/schema/kernel-3}creatorName'):
            node.text = fixed_name
    elif metadata['department'].item().split()[0] == 'Austin' and metadata['department'].item().split()[-1] == 'Department':
        fixed_name = metadata['department'].item().split(' ', 1)[1]
        for node in root.iter('{http://datacite.org/schema/kernel-3}creatorName'):
            node.text = fixed_name
    else:
        for node in root.iter('{http://datacite.org/schema/kernel-3}creatorName'):
            node.text = metadata['department'].item()

    # set description
    for node in root.iter('{http://datacite.org/schema/kernel-3}description'):
        node.text = metadata['desc'].item()
    # set resource type
    for node in root.iter('{http://datacite.org/schema/kernel-3}resourceType'):
        node.text = metadata['type'].item()
        node.set('resourceTypeGeneral', metadata['type'].item())

    xmlstr = ET.tostring(root, encoding='utf-8', method='xml')
    xml_encoded = base64.b64encode(xmlstr)

    return xml_encoded.decode('utf-8')


def publish_doi(socrata_4x4, temp_table=None, draft=True):
    """Publishes DOI, encodes XML into base64 and inserts DOI record into json"""
    import requests

    doi_assets = pd.read_json(doi_assets_json)
    url = 'https://api.datacite.org/dois'
    datacite_user = os.environ['datacite_user']
    datacite_pass = os.environ['datacite_pass']

    payload, doi, xml, metadata = assemble_payload(socrata_4x4, temp_table=temp_table, draft=draft)

    # publish new DOI
    r = requests.post(url, json=payload, auth=(datacite_user, datacite_pass))

    # update socrata asset's DOI metadata
    headers = {'Host': 'data.austintexas.gov',
               'Accept': """*/*""",
               'Content-Length': '6000',
               'Content-Type': 'application/json',
               'X-App-Token': os.environ['socrata_doi_app_token']}
    url = 'https://data.austintexas.gov/api/views/metadata/v1/{}'.format(socrata_4x4)
    data = {"customFields": {"Digital Object Identifer (DOI)": {"DOI Number": "https://doi.org/{}".format(doi)}}}
    r2 = requests.patch(url, json=data, auth=(os.environ['socrata_doi_user'], os.environ['socrata_doi_pass']), headers=headers)
    print(r2.content)

    if r.content[2:8] == 'errors' or r2.content[2:8] == 'errors':
        print('DataCite error \n')
        print(r.content)
        return False
    else:
        # update doi_assets json
        print(r.content)
        doi_assets = doi_assets.append({'socrata_4x4': socrata_4x4,
                                        'doi': doi,
                                        'metadata_xml': xml,
                                        'doi_prefix': doi.split('/')[-1].split('.')[0],
                                        'doi_suffix': doi.split('/')[-1].split('.')[1],
                                        'department': metadata['department'].item(),
                                        'created_at': str(datetime.datetime.now())}, ignore_index=True)
        doi_assets.to_json(doi_assets_json)
        return True


if __name__ == "__main__":
    pass
    # publish_doi('gric-78uy')

