#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)
import os
from pathlib import Path
from postgresql import ConnectionSettings
from postgresql import Database
from exporter import ExcelExporter
from exporter import  ExcelDecoration
from exporter import  ExcelDecorationElement
from exporter import ExcelDecorationManager
from exporter import SQLFileWriter
from forwarders import JIRAForwarder
from forwarders import JIRACommenter


class KranoExportError(Exception):
    """Raised when the Excel export encounters errors."""
    pass


class KranoDecorationError(Exception):
    """Raised when the Excel export encounters errors."""
    pass


class Krano(object):
    """Fetches records from a PostreSQL database and exports the result to one
    or several Excel documents. Can be configured to decorate the Excel documents
    with further worksheets containing additional informations and to upload the
    created Excel documents as attachments to a JIRA issue."""

    def __init__(self):
        self.db_connection_settings = None
        self.export_folderpath = None
        self.jira_base_url = None
        self.jira_user = None
        self.jira_password = None
        self.sql_decoration = True

    def set_database_config(self, connection_name, host, database_name, user, password):
        """Sets the database configuration.

        Args:
            connection_name (str): Name of the database connection.
            host (str): Host of the database server.
            database_name (str): Name of the database.
            user (str): Name of the database user.
            password (str): Password of the database user.
        """
        db_connection_settings = ConnectionSettings(connection_name, host, database_name, user, password)
        self.db_connection_settings = db_connection_settings

    def set_export_config(self, export_folderpath):
        """Sets the export configuration.

        Args:
            export_folderpath (str): Path to the export folder where the Excel & SQL files will be created.

        Raises:
            FileNotFoundError: The given export folder path does not exist.
            NotADirectoryError: The given export folder path does not point to a directory.
        """
        self.export_folderpath = export_folderpath

        if not os.path.exists(self.export_folderpath):
            errmsg = "No such file or directory: '{0}'".format(self.export_folderpath)
            raise FileNotFoundError(errmsg)

        if not os.path.isdir(self.export_folderpath):
            errmsg = "Chosen export folderpath does not point to a directory: '{0}'".format(self.export_folderpath)
            raise NotADirectoryError(errmsg)

    def set_jira_config(self, base_url, user, password):
        """Sets the basic JIRA configuration.

        Args:
            base_url (str): JIRA base URL, e.g. 'jira.domain.com'.
            user (str): Name of the JIRA user.
            password (str): Password of the JIRA user.
        """
        self.jira_base_url = base_url
        self.jira_user = user
        self.jira_password = password

    def export(self, sql_statement, xlsx_filename, sheet_name, chunk_size, overwrite_files=False, parallel_processes=2, excel_decorations=None, jira_issue=None):
        """Executes an SQL query against a PostgreSQL database and exports the fetched records to one or several Excel documents.

        Args:
            sql_statement (str): The SQL query to be executed.
            xlsx_filename (str): Filename of the Excel file to be created.
            sheet_name (str): Name of the worksheet where the records will occur in the Excel file.
            chunk_size (int): Maximum number of rows per Excel file.
            overwrite_files (bool): Indicates if an already existing Excel file will be overwritten.
            parallel_processes (int): The maximum count of parallel Excel export/decoration processes to be started, defaults to 2.
            excel_decorations (list): A list of ExcelDecoration objects. If this argument is ot given, the Excel files will not boe decorated.
            jira_issue (str): The JIRA issue where the created Excel & SQL files should be attached. If this argument is not given, no files will be uploaded.

        Raises:
            ValueError: No database configuration was set with set_database_config prior to calling the export function.
            ValueError: No export folderpath was defined with set_export_config prior to calling the export function.
            KranoExportError: Excel export encountered an error.
            KranoDecorationError: Excel decoration encountered an error.
        """
        if not self.db_connection_settings:
            errmsg = "No database configuration was set with set_database_config prior to calling the export function."
            raise ValueError(errmsg)

        if not self.export_folderpath:
            errmsg = "No export folderpath was defined with set_export_config prior to calling the export function."
            raise ValueError(errmsg)

        try:
            db = Database(self.db_connection_settings)
            result = db.query(sql_statement)
        except Exception as e:
            raise(e)
        finally:
            db.close()

        if result.record_count == 0:
            logger.info('The result from the database is empty')
            return

        xlsx_filepath = os.path.join(self.export_folderpath, xlsx_filename)
        sql_filepath = os.path.splitext(xlsx_filepath)[0] + '.sql'

        xlsx_exporter = ExcelExporter(xlsx_filepath, result, chunk_size, sheet_name, overwrite_files, parallel_processes=parallel_processes)
        xlsx_exporter_result = xlsx_exporter.export()

        if xlsx_exporter_result.has_errros():
            logger.error('The Excel export process encountered the following errors:')
            for excel_export_process_error in xlsx_exporter_result.excel_export_process_errors:
                logger.error('Process name: {0} | Filepath: {1} | Error message: {2}'.format(excel_export_process_error.excel_export_process.process_name,
                                                                                             excel_export_process_error.excel_export_process.filepath,
                                                                                             excel_export_process_error.message))
            raise KranoExportError('The Excel export process encountered errors.')

        exported_xlsx_filepaths = [res.filepath for res in xlsx_exporter_result.excel_export_process_results]

        copy_excel_decorations = excel_decorations.copy()

        if excel_decorations:
            if self.sql_decoration:
                excel_sql_decoration = ExcelDecoration('SQL', "Query details")
                excel_sql_decoration.add_element(ExcelDecorationElement('Query duration', result.query_duration))
                excel_sql_decoration.add_element(ExcelDecorationElement('SQL query', result.sql_statement))
                copy_excel_decorations.append(excel_sql_decoration)

            excel_decorator_manager = ExcelDecorationManager(exported_xlsx_filepaths, copy_excel_decorations, parallel_processes=parallel_processes)
            xlsx_decorator_result = excel_decorator_manager.decorate()

            if xlsx_decorator_result.has_errros():
                logger.error('The Excel decoration process encountered the following errors:')
                for excel_decoration_process_error in xlsx_decorator_result.excel_decoration_process_errors:
                    logger.error('Process name: {0} | Filepath: {1} | Error message: {2}'.format(excel_decoration_process_error.excel_decorator.process_name,
                                                                                                 excel_decoration_process_error.excel_decorator.filepath,
                                                                                                 excel_decoration_process_error.message))
                raise KranoDecorationError('The Excel decoration process encountered errors.')

        sql_exporter = SQLFileWriter(sql_filepath, result.sql_statement)
        sql_exporter.write()

        if jira_issue:
            upload_filepaths = exported_xlsx_filepaths + [sql_filepath]
            jira_forwarder = JIRAForwarder(self.jira_base_url, self.jira_user, self.jira_password)
            jira_forwarder.upload(jira_issue, upload_filepaths)

            upload_filenames = [' [^' + Path(filepath).name + ']' for filepath in upload_filepaths]
            comment_filenames = '\n'.join(upload_filenames)
            comment = 'The Python script krano attached the following {0} file(s) to this JIRA issue: \n\n{1}'.format(len(upload_filenames), comment_filenames)
            jira_commenter = JIRACommenter(self.jira_base_url, self.jira_user, self.jira_password)
            jira_commenter.comment(jira_issue, comment)
