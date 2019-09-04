#/usr/bin/python
# -*- coding: utf-8 -*-

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)
import requests
import json


class JIRAForwarder(object):
    """Uploads files as attachments to a JIRA issue.

    Args:
        base_url (str): JIRA base URL, e.g. 'jira.domain.com'.
        login (str): Name of the JIRA user.
        password (str): Password of the JIRA user.
    """
    def __init__(self, base_url, login, password):
        self.base_url = base_url
        self.login = login
        self.password = password

    def upload(self, issue, filepaths):
        """Uploads files as attachments to a specific JIRA issue.

        Args:
            issue (str): Code of the JIRA issue, e.g. 'ITD-122'
            filepaths (list): A list of filepaths to upload as attachments to the JIRA issue.
        """
        url = 'https://{0}/rest/api/2/issue/{1}/attachments'.format(self.base_url, issue)
        headers = {"X-Atlassian-Token": "nocheck"}

        logger.info('+{0}+'.format(60 * '-'))

        for filepath in filepaths:
            logger.info("Uploading file at {0} to {1}...".format(filepath, url))
            try:
                openfile = open(filepath, 'rb')
                files = {'file': openfile}
                r = requests.post(url, auth=(self.login, self.password), files=files, headers=headers)
                logger.info("HTTP status code: {0}".format(r.status_code))
                r.raise_for_status()
            except Exception as e:
                logger.warning("Upload failed: {0}".format(str(e)))
            finally:
                openfile.close()


class JIRACommenter(object):
    """Adds comments to a JIRA issue.

    Args:
        base_url (str): JIRA base URL, e.g. 'jira.domain.com'.
        login (str): Name of the JIRA user.
        password (str): Password of the JIRA user.
    """
    def __init__(self, base_url, login, password):
        self.base_url = base_url
        self.login = login
        self.password = password

    def comment(self, issue, comment):
        """Adds a comment to a specific JIRA issue.

        Args:
            issue (str): Code of the JIRA issue, e.g. 'ITD-122'
            comment (str): A comment to be added to the JIRA issue.
        """
        url = 'https://{0}/rest/api/2/issue/{1}/comment'.format(self.base_url, issue)
        headers = {'Content-Type':'application/json'}
        data = json.dumps({'body': comment})

        logger.info('+{0}+'.format(60 * '-'))

        logger.info("Adding comment to {0}...".format(url))
        try:
            r = requests.post(url, auth=(self.login, self.password), data=data, headers=headers)
            logger.info("HTTP status code: {0}".format(r.status_code))
            r.raise_for_status()
        except Exception as e:
            logger.warning("Adding the comment failed: {0}".format(str(e)))