# krano
krano is a Python script that exports data from a PostgreSQL database into Microsoft Excel documents.

In addition it can also attach the created Excel documents to a specified JIRA issue and add an appropriate comment.

When the data is so big that krano needs to create several Microsoft documents, it will do so in **parallel** processes to reduce creation time.

You can see krano in action here:

[Short krano video](https://twitter.com/applescripter/status/1118601160263372802)

I develop krano in my private time because my colleagues from the business departments simply love Excel documents. And as a data analyst I want to see them happy. As the saying goes: "Microsoft Excel is the laser sword of royalty collecting societies!"

## Dependencies
In order to use krano you will need the following software, libraries and modules:

* Python 3.6 or higher
	* [psycopg2](http://initd.org/psycopg/) (for connecting to PostgreSQL)
	* [pandas](https://pandas.pydata.org) (for data transformations)
	* [openpyxl](https://openpyxl.readthedocs.io/en/stable/) (for decorating the Excel documents)
	* [PrettyTable](http://zetcode.com/python/prettytable/)

For ease of use I recommend using krano with:

* [PyCharm](https://www.jetbrains.com/pycharm/download/) (Community Edition)


## Usage
You can use krano in two different ways: From within PyCharm or from the command line. In both cases you have to configure it first to your own environment. Here is how you do that:

### Configuration
The file named *config.py* contains all settings to adjust krano to your own environment.
#### Database(s)
The dictionary named DATABASE\_CONNECTION\_SETTINGS contains the details about the database connections you want to use with krano. 

I included three sample connection settings to give you an idea about how to set it up.

Please manage the password of your database credentials using the separate file *secrets.py* as shown in the sample connection settings.

#### Creation of Excel documents
The following variables in the config.py file allow to configure the creation of Excel documents:

* EXPORT\_FOLDERPATH: This directory will be used to store the created Excel documents and SQL files. Change it to one on your computer.
* EXPORT\_OVERWRITE\_FILES: If set to True, krano will overwrite existing files in the directory specified in EXPORT\_FOLDERPATH. If set to False, krano will not overwrite existing files.
* EXPORT\_PARALLEL\_PROCESSES: The maximum number of parallel processes to create Excel documents. Change this with caution. On my private MacBook Pro I can easily set this number to 9. On my Dell computer at work I can only use 3 parallel processes.
* XLSX\_SHEET˜_NAME: The name of the worksheet within the Excel document containing the data.

#### JIRA
If you want krano to attach the created Excel documents to a specified JIRA issue, then you need to adjust the following settings to your needs:

* JIRA\_BASE\_URL: The base URL of your JIRA application
* JIRA\_USER: Your JIRA user name
* JIRA\_PASSWORD: Your JIRA password. Please use the *secrets.py* file to store it, like its shown in the sample configuration.

### Using krano with PyCharm

1. Open the krano directory with PyCharm
2. Specify the Python interpreter for the krano project (just in case you did not do it before)
3. Click on the file *valvo.py* to edit it
4. In the method main\_single() set the variable
	5. *creator* to your name
	6. *chunk_size* to the maximum count of lines saved in one Excel document
	7. *conn_name* to the name of the database connection you want to use (see file *config.py* for database connection settings)
	8. *jira_issue* to the identifier of the JIRA issue where you want to attach the created Excel document(s)
	9. *jira_title* to the title of the JIRA issue where you want to attach the created Excel document(s)
	10. *xlsx_filename* to the name you want to use for the Excel document(s) to be created
11. Click on the file *sql.py* and enter your SQL query between the three quotes of the variable *SQL\_STATEMENT*
12. Click on the file *valvo.py* and then choose from the PyCharm menu **Run > Run > Run valvo**
13. Now krano will start to fetch the data from the database and export it to Excel documents
14. If you do not want krano to upload the created Excel documents to JIRA, then just set the last argument of this method call in *main_single()* to None: `krano.export(sql_statement, xlsx_filename, config.XLSX_SHEET_NAME, chunk_size, config.EXPORT_OVERWRITE_FILES, config.EXPORT_PARALLEL_PROCESSES, excel_decorations, None)`

### Using krano with the command line
Configure the *valvo.py* and *sql.py* files as given above (for using krano with PyCharm) and then use your Python version in the command line to execute the *valvo.py* file.