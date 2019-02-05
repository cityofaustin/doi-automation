# DOI Automation

Datacite (https://en.wikipedia.org/wiki/DataCite) is a non profit organization which provides an easy way to register, cite, and access datasets online.
The City of Austin would like to use this organization's tools to garner insight into the usage of our open data as well as give the public a simple and effective way to cite our open data for any use.

To see some examples of our citations see:
https://search.datacite.org/members/austintx

This project's goal is to explore and implement an integration between DataCite's citation repository and the city's Socrata Open Data portal (https://data.austintexas.gov/). This will be done by developing automation to synchronize datacite's DOI repository with the City of Austin's socrata portal assets and metadata using the two organization's APIs and a python backend.

Socrata Discovery API:
https://socratadiscovery.docs.apiary.io/#

DataCite REST API:
https://support.datacite.org/docs/api

# Getting Started:

# _Install python 3.6_

https://www.python.org/downloads/release/python-360/

# _Python dependencies_

requests:

http://docs.python-requests.org/en/master/

```
pip install requests
```

pandas:

https://pandas.pydata.org/

```
pip install pandas
```

# _Clone this repository_

```
git clone https://github.com/cityofaustin/doi-automation.git
```

## Run with Docker

```bash
docker build --tag=doi-automation .
docker run --rm -it --name doi-automation doi-automation
```
