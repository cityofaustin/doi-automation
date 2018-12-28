import os
import datetime


import pandas as pd


fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'data\\doi_assets.json')
doi_assets_json = os.path.abspath(os.path.realpath(filename))

filename = os.path.join(fileDir, 'data\\socrata_assets.json')
socrata_assets_json = os.path.abspath(os.path.realpath(filename))

filename = os.path.join(fileDir, 'data\\departments.json')
departments_json = os.path.abspath(os.path.realpath(filename))

filename = os.path.join(fileDir, 'data\\datacite-example.xml')
datacite_xml = os.path.abspath(os.path.realpath(filename))


def assemble_payload(socrata_4x4, draft=True, update=False):
    """Assembles json for doi creation post & update put. Identifies dept prefix and doi suffix."""

    # Gather static data
    socrata_assets = pd.read_json(socrata_assets_json)
    departments = pd.read_json(departments_json)
    doi_assets = pd.read_json(doi_assets_json)

    # fetch metadata
    metadata = socrata_assets.loc[socrata_assets['socrata_4x4'] == socrata_4x4]

    # figure out which department prefix to use
    dept_prefix = departments.loc[departments['Department'] == metadata['department'].item()]

    if update is False:
        # figure out next doi suffix to use if new
        department_dois = doi_assets.loc[doi_assets['department'] == metadata['department'].item()]
        # add 1 to highest suffix value and use zfill to pad w/zeros to 6 chars
        doi_suffix = str((department_dois['doi_suffix'].max()+1)).zfill(6)
        # zfill dept prefix to 3 chars
        doi = '10.26000/{}.{}'.format(str(dept_prefix.index[0]).zfill(3), doi_suffix)
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

    #TODO not working =(
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
    # set creator/dept

    # add City Of to departments that start with 'Austin'
    if metadata['department'].item().split()[0] == 'Austin':
        fixed_name = 'City Of {}'.format(metadata['department'].item())
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


def publish_doi(socrata_4x4, draft=True):
    """Publishes DOI, encodes XML into base64 and inserts DOI record into postgres"""
    import requests

    doi_assets = pd.read_json(doi_assets_json)
    url = 'https://api.datacite.org/dois'
    datacite_user = os.environ['datacite_user']
    datacite_pass = os.environ['datacite_pass']

    payload, doi, xml, metadata = assemble_payload(socrata_4x4, draft)

    # publish new DOI
    r = requests.post(url, json=payload, auth=(datacite_user, datacite_pass))
    if r.content[2:8] == 'errors':
        print('DataCite error \n')
        print(r.content)
        return
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


if __name__ == "__main__":
    pass
    # publish_doi('rfif-mmvg')

