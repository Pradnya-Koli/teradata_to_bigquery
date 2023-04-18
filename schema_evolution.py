  GNU nano 4.8                                                                                                                                                                                                                                                                                                             schema_Evolution.py                                                                                                                                                                                                                                                                                                                        
import teradatasql
import json
import pandas as pd
import re
import logging
from google.cloud import bigquery
from Create_BigQuery_Table import *
import os 
from google.cloud import storage
dataset_id = os.environ.get('dataset_name')
project_id = os.environ.get('project_id')
bucket_name = os.environ.get('bucket_name')
host = os.environ.get('host')
username = os.environ.get('username')
password = os.environ.get('password')
database =  "testdrive"
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)
def schema_evolution():
    try:
        project = 'searce-practice-data-analytics'
        client = bigquery.Client(project=project)
        logger = logging.getLogger(__name__)
        with open("config/teradata_config.json") as config_file:
            open('/opt/teradata_to_bigquery/schema_changes/teradata_changes.sql', 'w').close()
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
            f = open("/opt/teradata_to_bigquery/schema_changes/teradata_changes.sql", "a+")
            f.write(result)
            f.close()
            word = 'UPDATE'
            open('/opt/teradata_to_bigquery/schema_changes/filter_changes.sql', 'w').close()
            with open(r'/opt/teradata_to_bigquery/schema_changes/teradata_changes.sql', 'r') as fp:
                lines = fp.readlines()
                for line in lines:
                    if line.find(word) != -1:
                        queries = str(line)
                        queries = queries.replace('"', '')
                        print(queries)
                        op = open('/opt/teradata_to_bigquery/schema_changes/filter_changes.sql', 'a+')
                        op.write(queries)
                        op.close()

            word = 'ALTER'
            #            open('filter_changes.sql', 'w').close()
            with open(r'/opt/teradata_to_bigquery/schema_changes/teradata_changes.sql', 'r') as fp:
                lines = fp.readlines()
                for line in lines:
                    if line.find(word) != -1:
                        queries = str(line)

                        print(queries)
                        op = open('/opt/teradata_to_bigquery/schema_changes/filter_changes.sql', 'a+')
                        op.write(queries)
                        op.close()

            open('/opt/teradata_to_bigquery/Source_DDL/source_ddl.sql', 'w').close()
            words = 'None'
            with open(r'/opt/teradata_to_bigquery/schema_changes/filter_changes.sql', 'r') as rd:
                lines = rd.readlines()
                for line in lines:
                    if line.find(words) != -1:
                        success_query = str(line)
                        success_query = success_query.replace("[", "").replace(", None", "").replace("'", "")
                        print(success_query)

                        succes = open('/opt/teradata_to_bigquery/Source_DDL/source_ddl.sql', 'a+')
                        succes.write(success_query)
                        succes.close()
 #           with open(r'/opt/teradata_to_bigquery/Source_DDL/source_ddl.sql', 'r') as rd:
#            word='UPDATE'


    except Exception as e:
        logger.exception('Failed to open database connection.')
        print("Failed")


def formatting_update():
    with open('/opt/teradata_to_bigquery/BigQuery_Compatible_DDL/source_ddl.sql', 'r') as rd:
        lines = rd.readlines()
        words ='UPDATE'
        for line in lines:
            if line.find(words) != -1:
               format_query=str(line)
               replace_query=("{}").format(database)
               format_query=format_query.replace(replace_query,"")
               final_update_query = open('/opt/teradata_to_bigquery/BigQuery_Compatible_DDL/source_ddl.sql', 'a+')

               final_update_query.write(format_query)
               final_update_query.close()

schema_evolution()

