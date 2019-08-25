#/usr/bin/python
# -*- coding: utf-8 -*-

import secrets

DATABASE_CONNECTION_SETTINGS = {
    'Database DEV': {'connection_name': 'Database DEV', 'host': 'db_dev.evilcompany.local',
                    'database_name': 'some_database', 'user': 'someone', 'password': secrets.DB_PASSWORDS['Database DEV']},
    'Database TEST': {'connection_name': 'Database TEST', 'host': 'db_test.evilcompany.local',
                    'database_name': 'some_database', 'user': 'someone', 'password': secrets.DB_PASSWORDS['Database TEST']},
    'Database PROD': {'connection_name': 'Database REF', 'host': 'db_prod.evilcompany.local',
                    'database_name': 'some_database', 'user': 'someone', 'password': secrets.DB_PASSWORDS['Database PROD']}
}

EXPORT_FOLDERPATH = '/Users/someone/Desktop/krano_export/'
EXPORT_OVERWRITE_FILES = True
EXPORT_PARALLEL_PROCESSES = 3
XLSX_SHEET_NAME = 'Data'

JIRA_BASE_URL = 'jira.evilcompany.com'
JIRA_USER = 'Someone'
JIRA_PASSWORD = secrets.JIRA_PASSWORD