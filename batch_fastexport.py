import os
import teradatasql
import teradataml
from teradataml import create_context, remove_context
from teradataml.dataframe.dataframe import DataFrame
from teradataml import fastexport
import pandas as pd
import threading
import datetime
from google.cloud import storage
from google.cloud import bigquery
import time
import pytz
from google.cloud.exceptions import NotFound

start_time = time.time()
sink_bucket=os.environ.get('sink_bucket_teradata')
dataset_id = os.environ.get('dataset_name')
project_id = os.environ.get('project_id')
bucket_name = os.environ.get('bucket_name')
host = os.environ.get('host')
username = os.environ.get('username')
password = os.environ.get('password')
database = os.environ.get('database')
audit_column = os.environ.get('audit')

tz = pytz.timezone('Asia/Kolkata')
# Get the current datetime with the timezone
current_time = datetime.datetime.now(tz)
current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
print(current_time_str)

bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)


cnxn = teradatasql.connect(host=host, user=username, password=password, database=database)

cur = cnxn.cursor()
td_context = create_context(host=host, username=username, password=password, database=database)
# create a DataFrame object that points to your table
query=("SELECT  TableName FROM  DBC.TablesV WHERE   TableKind = 'T' and DatabaseName = '{}'").format(database)
list_table_df = teradataml.DataFrame.from_query(query)
print(list_table_df)

table_df = list_table_df.to_pandas()  # convert to pandas DataFrame
list_table = table_df["TableName"].tolist()  # extract column as list
print(list_table)


def fetch_all_data(list_table):
    for row in list_table:
        fetch_data(row)


def fetch_data(table_name):
    client = bigquery.Client(project=project_id)
    df = DataFrame(table_name)
    out = fastexport(df)
    print(out)
    print("done")
    get_col_name_query="select columnname from dbc.columns where tablename ='{}' and databasename = '{}';".format(table_name,database)
    list_table_df = teradataml.DataFrame.from_query(get_col_name_query)
    csv_data = out.to_csv(index=False)

    # Create a client to interact with the storage service
    bucket = storage.Client().bucket(bucket_name)
   # client = storage.Client(project=project_id)
    folder_name = current_time_str + '/' + database + '/' + table_name
    # Upload the CSV data to the bucket
    filename = table_name + ".csv"
    blob = bucket.blob(folder_name + '/' + filename)
    blob.upload_from_string(csv_data)

    print(f"DataFrame uploaded to {bucket_name}")
    # Get a reference to the CSV file in GCS

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    table_id = "{}.{}.{}".format(project_id, database, table_name)

    # Load the data from the CSV file into a BigQuery table
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Infer schema from the CSV file
        skip_leading_rows=1,  # Skip the CSV header row
        source_format=bigquery.SourceFormat.CSV,
    )
    """
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{filename}", f"{dataset_id}.{table_name}", job_config=job_config
    )
    """
    uri = "gs://{}/{}/{}/{}/*".format(bucket_name, current_time_str, database, table_name)
    load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config)
    load_job.result()
    table = client.get_table(table_id)
    print("Loaded {} rows in {}".format(table.num_rows, table_name))

    # Wait for the job to complete
#    job.result()

    # Check the status of the job
 #   if job.errors:
  #      raise Exception(job.errors)

    print(f"Data loaded into {dataset_id}.{table_name} successfully.")

    create_Metadata_table(database)
    populate_metadata_table(database, table_name, audit_column, cnxn)
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

def populate_metadata_table(Dataset_name, Table_name, audit_colum, conn):
    client = bigquery.Client(project_id)
    sql = "SELECT max({}) FROM {}.{}.{} ".format(audit_colum, project_id, Dataset_name, Table_name)
    print(sql)
    ans = list(client.query(sql))
    print(ans[0][0])
    ans = str(ans[0][0])
    print("Max value of {} in {}.{} : ".format(audit_colum, Dataset_name, Table_name), ans)
    cur = conn.cursor()
    query = "select max({}) from {}.{};".format(audit_column,database, Table_name)
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

def process_table(table_df):
    for table in table_df:
        print(table, "------------------------ process_table ------------------------------")
        fetch_data(table)


threads = []
batch_size = 70

start_time = time.time()
for i in range(0, len(list_table), batch_size):
    batch_tables = list_table[i:i + batch_size]
    print("-----------------------------{batch_tables}------------------------------------")
    thread = threading.Thread(target=process_table, args=[batch_tables])
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

end_time = time.time()
print("Time taken:", end_time - start_time, "seconds")
