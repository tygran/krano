#!/usr/bin/python
# -*- coding: utf-8 -*-

import config
import sql
from krano import Krano
from exporter import  ExcelDecoration
from exporter import  ExcelDecorationElement
import jira


def main_single():
    creator = 'Your Name'
    chunk_size = 250000
    conn_name = 'Database PROD'
    db_config = config.DATABASE_CONNECTION_SETTINGS[conn_name]
    jira_issue = 'SMP-999'
    jira_title = jira.getissuetitle(config.JIRA_BASE_URL, jira_issue, config.JIRA_USER , config.JIRA_PASSWORD)
    xlsx_filename = 'Data_export_{0}_{1}.xlsx'.format(conn_name.replace(' ', '_'), jira_issue)

    sql_statement = sql.SQL_STATEMENT

    excel_decorations = []
    excel_info_decoration = ExcelDecoration('Info', jira_title)
    excel_info_decoration.add_element(ExcelDecorationElement('Created on', 'CURRENT_DATETIME'))
    excel_info_decoration.add_element(ExcelDecorationElement('Created by', creator))
    excel_info_decoration.add_element(ExcelDecorationElement('', ''))
    excel_info_decoration.add_element(ExcelDecorationElement('Server', db_config['host']))
    excel_info_decoration.add_element(ExcelDecorationElement('Database',  db_config['database_name']))
    excel_info_decoration.add_element(ExcelDecorationElement('JIRA-URL', "https://{0}/{1}".format(config.JIRA_BASE_URL, jira_issue)))
    excel_decorations.append(excel_info_decoration)

    krano = Krano()
    krano.set_database_config(db_config['connection_name'], db_config['host'], db_config['database_name'], db_config['user'], db_config['password'])
    krano.set_export_config(config.EXPORT_FOLDERPATH)
    krano.set_jira_config(config.JIRA_BASE_URL, config.JIRA_USER, config.JIRA_PASSWORD)
    krano.export(sql_statement, xlsx_filename, config.XLSX_SHEET_NAME, chunk_size, config.EXPORT_OVERWRITE_FILES, config.EXPORT_PARALLEL_PROCESSES, excel_decorations, jira_issue)


if __name__ == '__main__':
    main_single()