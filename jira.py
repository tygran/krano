# /usr/bin/python
# -*- coding: utf-8 -*-

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)
import requests
import json


def getissuetitle(base_url, issue, login, password):
    """Fetches the title/summary of a given JIRA issue.

    Args:
        base_url (str): JIRA base URL, e.g. 'jira.domain.com'.
        issue (str): JIRA issue key.
        login (str): Name of the JIRA user.
        password (str): Password of the JIRA user.

    Returns:
        The title of the given JIRA issue,
    """
    try:
        url = 'https://{0}/rest/api/2/issue/{1}'.format(base_url, issue)
        r = requests.get(url, auth=(login, password))
        r.raise_for_status()
        issuetitle = r.json()['fields']['summary']
        return issuetitle
    except Exception as e:
        logger.warning("GET request to JIRA failed: {0}".format(str(e)))
        raise (e)
