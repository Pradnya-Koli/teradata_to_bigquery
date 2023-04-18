import datetime
import teradataml
from teradataml import create_context, remove_context
from teradataml.dataframe.dataframe import DataFrame
from teradataml import fastexport
import pandas as pd
import threading
from google.cloud import storage
from google.cloud import bigquery
import time
import teradatasql
import os
import pytz
from google.cloud.exceptions import NotFound

audit_column=os.environ.get('audit')
start_time = time.time()
sink_bucket=os.environ.get('sink_bucket_teradata')
dataset_id = os.environ.get('dataset_name')
project_id = os.environ.get('project_id')
bucket_name = os.environ.get('bucket_name')
host = os.environ.get('host')
username = os.environ.get('username')
password = os.environ.get('password')
database = os.environ.get('database')
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)

lastest_timestamp = "lastest_timestamp"
td_context = create_context(host=host, username=username, password=password, database=database)
cnxn = teradatasql.connect(host=host, user=username, password=password, database=database)

cur = cnxn.cursor()
count_query = ("SELECT COUNT(*) AS table_count FROM dbc.tables WHERE tablekind = 'T' AND databasename = '{}'").format(
    database)
cur.execute(count_query)

table_count = int(cur.fetchone()[0])
print(type(table_count))
print(table_count)

query_01 = "SELECT  TableName FROM  DBC.TablesV WHERE   TableKind = 'T' and DatabaseName = '{}'".format(database)
list_table_df = teradataml.DataFrame.from_query(query_01)
print(list_table_df)
table_df = list_table_df.to_pandas()  # convert to pandas DataFrame
list_table = table_df["TableName"].tolist()  # extract column as list
print(list_table)


# Set the timezone
tz = pytz.timezone('Asia/Kolkata')
# Get the current datetime with the timezone
current_time = datetime.datetime.now(tz)
current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
print(current_time_str)


def fetch_all_data(list_table_batch):
    for table_name in list_table_batch:
        fetch_data(table_name)


def fetch_data(table_name):
    client = bigquery.Client(project=project_id)

    bq_lastest = "SELECT max({}) FROM {}.{}.{} ".format('Maxvalue', project_id, database, 'metadata')
    bq_lastest_tm = client.query(bq_lastest)
    res_bq = bq_lastest_tm.result()
    for row in res_bq:
        print(row, "-----------biqquery max tm--------------------------------")
        text = str(row)
        final_tm_bq = text.replace("Row((", " ").replace(",),", " ").replace("{'f0_': 0})", " ").replace("+00:00", " ").replace("'","")
        print(final_tm_bq)
        final_tm=final_tm_bq
    
        get_col_name_query = "select columnname from dbc.columns where tablename ='{}' and databasename = '{}';".format(
        table_name, database)
        print( get_col_name_query )

        query = "select * from {}.{} where {} > '{}' ".format(database,table_name, audit_column, final_tm_bq)
        print(query)
        cur.execute(query)
        data_res = cur.fetchall()
        print(data_res)
        df = teradataml.DataFrame.from_query(query)
        df = DataFrame(table_name)
        out = fastexport(df)

        csv_data = out.to_csv(index=False)

        bucket = storage.Client().bucket(bucket_name)

        folder_name = current_time_str + '/' + database + '/' + table_name
        bucket = storage_client.get_bucket(bucket_name)
        filename = table_name + ".csv"

        blob = bucket.blob(folder_name + '/' + filename)
    # filename = table_name + ".csv"
        blob.upload_from_string(csv_data)

        print(f"DataFrame uploaded to {bucket_name}")
    # Get a reference to the CSV file in GCS

    # bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(filename)
        table_id = "{}.{}.{}".format(project_id, database, table_name)
  
    # Load the data from the CSV file into a BigQuery table
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Infer schema from the CSV file
            skip_leading_rows=1,  # Skip the CSV header row
            source_format=bigquery.SourceFormat.CSV,
        )

        uri = "gs://{}/{}/{}/{}/*".format(bucket_name, current_time_str, database, table_name)
        load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config)
        load_job.result()
        table = client.get_table(table_id)
        print("Loaded {} rows in {}".format(table.num_rows, table_name))
        create_Metadata_table(database)
        update_metadata_table(database, table_name, audit_column,cnxn)
        backup_metadata_table(database)
def create_Metadata_table(dataset_detail):
    client = bigquery.Client(project_id)
    schema = [
        bigquery.SchemaField("Table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Column_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Maxvalue", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Last_Schema_Check", "TIMESTAMP", mode="REQUIRED"),
    ]

    try:
        table = bigquery.Table('{}.{}.metadata'.format(project_id, dataset_detail), schema=schema)
        print(client.get_table(table))
        print("Metadata Table Does not exist")
        print("Creating Metadata Table")
    except NotFound:
        try:
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        except:
            print("Metadata already exists")
            pass


def update_metadata_table(Dataset_name, Table_name, audit_colum, conn):
    client = bigquery.Client(project_id)
    sql = "SELECT max({}) FROM {}.{}.{} ".format(audit_colum, project_id, Dataset_name, Table_name)
    print(sql)
    ans = list(client.query(sql))
    # print(ans[0][0])
    ans = str(ans[0][0])
    print("Max value of {} in {}.{} : ".format(audit_colum, Dataset_name, Table_name), ans)
    cur = conn.cursor()
    query = "select max({}) from {};".format(audit_column, Table_name)
    cur.execute(query)
    res = cur.fetchone()
    print(res[0])
    sql = "Select * from {}.{}.metadata where Table_name='{}'".format(project_id, Dataset_name,
                                                                      Table_name)
    print(sql)
    val = list(client.query(sql))
    if len(val) != 0:
        sql = "update {}.{}.metadata  set Maxvalue='{}', Last_Schema_Check='{}' " \
              " where Table_name='{}'".format(project_id, Dataset_name, ans, res[0], Table_name)
        print(sql)
    else:
        sql = "INSERT INTO {}.{}.metadata values('{}','{}','{}','{}')".format(project_id,
                                                                              Dataset_name, Table_name, audit_colum,
                                                                              ans, res[0])
    print(sql)
    client.query(sql)
    print("Metadata table Updated")


def backup_metadata_table(dataset_name):
    dataset_id = dataset_name
    table_id = "metadata"
    client = bigquery.Client(project_id)
    destination_uri = "gs://{}/{}/metadata_backup/{}".format(sink_bucket, dataset_name,
                                                             "metadata.csv")
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project_id, dataset_id, table_id, destination_uri)
    )


threads = []
batch_size = 1

start_time = time.time()
for i in range(0, len(list_table), batch_size):
    batch_tables = list_table[i:i + batch_size]
    print("-----------------------------{batch_tables}------------------------------------")
    thread = threading.Thread(target=fetch_all_data, args=[batch_tables])
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()
