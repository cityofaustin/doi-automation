import os
import requests


import psycopg2

from publish_doi import assemble_payload


def update_doi(conn, socrata_4x4, identifier):
    """Updates existing DOI's metadata if diff detects a change"""

    url = 'https://api.datacite.org/dois/'
    datacite_user = os.environ['datacite_user']
    datacite_pass = os.environ['datacite_pass']

    payload, xml, metadata = assemble_payload(conn, socrata_4x4, update=True)
    cur = conn.cursor()

    try:
        cur.execute("""UPDATE doi_assets 
                       SET xml = %s, last_updated = NOW()
                       WHERE socrata_4x4 = %s""", (xml, socrata_4x4))

        r = requests.put('{}{}'.format(url, identifier), auth=(datacite_user, datacite_pass))
    except psycopg2.IntegrityError:
        pass

    conn.commit()
    cur.close()


if __name__ == "__main__":
    conn = psycopg2.connect(host="localhost", database="citation-station",
                            user=os.environ['postgres_user'],
                            password=os.environ['postgres_pass'])

    update_doi(conn, '4hh5-fx4w')

