import teradatasql
import json
import pandas as pd
import re
import logging
from google.cloud import bigquery
from Create_BigQuery_Table import *

database_name = os.environ.get('database')

def teradata_ddl_extractor():
    try:
        project = 'searce-practice-data-analytics'
        client = bigquery.Client(project=project)
        logger = logging.getLogger(__name__)
        with open("config/teradata_config.json") as config_file:
            open('Source_DDL/source_ddl.sql', 'w').close()
            data = json.load(config_file)
            server = data['details']['host']
            database = data['details']['database']
            username = data['details']['username']
            password = data['details']['password']

            cnxn = teradatasql.connect(host=server, user=username, password=password, database=database)

            cursor = cnxn.cursor()
            query2 = "SELECT  TableName FROM  DBC.TablesV WHERE   TableKind = 'T' and     DatabaseName = '{}' ORDER BY    TableName;".format(database_name)
            cursor.execute(query2)
            res_Database = cursor.fetchall()
            table_list = pd.DataFrame(res_Database)
            
            create_bq_dataset(dataset_name)

            query = "Select RequestText from dbc.tables where tablekind='T' and databasename='Test'"
            cursor.execute(query)
            tables = cursor.fetchall()
            text = str(tables)
            text = re.sub(r'[""]', '', text, flags=re.IGNORECASE)

            final = text.replace("[[", " ").replace("\\r", "\n").replace("]]", "").replace("'", "").replace("[",
                                                                                                            "").replace(
                "]", "").replace(";,", ";").replace("Test.", "").replace(";", ";").replace("CREATE TABLE",
                                                                                           "CREATE TABLE Test.").replace(
                "Test. ", "Test.")
            teradataddl = ""

            f = open("Source_DDL/source_ddl.sql", "a+")
            f.write(final + "\n")
            f.close()
    except Exception as e:
        logger.exception('Failed to open database connection.')
        print("Failed")


teradata_ddl_extractor()
