import teradatasql
import json
import pandas as pd
import re
import logging
from google.cloud import bigquery
from Create_BigQuery_Table import *


def schema_evolution():
    try:
        project = 'searce-practice-data-analytics'
        client = bigquery.Client(project=project)
        logger = logging.getLogger(__name__)
        with open("config/teradata_config.json") as config_file:
            open('schema_change/steradata_changes.sql', 'w').close()
            data = json.load(config_file)
            server = data['details']['host']
            database = data['details']['database']
            username = data['details']['username']
            password = data['details']['password']

            cnxn = teradatasql.connect(host=server, user=username, password=password, database=database)

            cursor = cnxn.cursor()
            #  query2 = "SELECT  TableName FROM  DBC.TablesV WHERE   TableKind = 'T' and     DatabaseName = 'Test' ORDER BY    TableName;"
            query2 = "SELECT  QueryText,ErrorText FROM    DBC.QryLogV WHERE   StartTime >  CURRENT_TIMESTAMP - INTERVAL '5' HOUR  ORDER BY  StartTime DESC;"
            cursor.execute(query2)
            result = cursor.fetchall()
            print(result)
            result = str(result)
            result = result.replace("],", "\n")
            f = open("schema_changes/teradata_changes.sql", "a+")
            f.write(result)
            f.close()
            word = 'UPDATE'
            open('schema_changes/filter_changes.sql', 'w').close()
            with open(r'schema_changes/teradata_changes.sql', 'r') as fp:
                lines = fp.readlines()
                for line in lines:
                    if line.find(word) != -1:
                        queries = str(line)
                        queries = queries.replace('"', '')
                        print(queries)
                        op = open('filter_changes.sql', 'a+')
                        op.write(queries)
                        op.close()

            word = 'ALTER'
            #            open('filter_changes.sql', 'w').close()
            with open(r'schema_changes/teradata_changes.sql', 'r') as fp:
                lines = fp.readlines()
                for line in lines:
                    if line.find(word) != -1:
                        queries = str(line)

                        print(queries)
                        op = open('filter_changes.sql', 'a+')
                        op.write(queries)
                        op.close()

            open('Source_DDL/source_ddl.sql', 'w').close()
            words = 'None'
            with open(r'schema_changes/filter_changes.sql', 'r') as rd:
                lines = rd.readlines()
                for line in lines:
                    if line.find(words) != -1:
                        success_query = str(line)
                        success_query = success_query.replace("[", "").replace(", None", "").replace("'", "")
                        print(success_query)

                        succes = open('Source_DDL/source_ddl.sql', 'a+')
                        succes.write(success_query)
                        succes.close()
    except Exception as e:
        logger.exception('Failed to open database connection.')
        print("Failed")


schema_evolution()



