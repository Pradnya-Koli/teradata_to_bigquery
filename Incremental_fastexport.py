from teradataml.exceptions import UdaExecError
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

start_time = time.time()

dataset_id = os.environ.get('dataset_name')
project_id = os.environ.get('project_id')
bucket_name = os.environ.get('bucket_name')
host = os.environ.get('host')
username = os.environ.get('username')
password = os.environ.get('password')
database = os.environ.get('database')
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)

td_context = create_context(host='localhost', username='dbc', password='dbc', database='Test')
cnxn = teradatasql.connect(host="localhost", user="dbc", password="dbc", database="Test")

cursor = cnxn.cursor()
cursor.execute("SELECT COUNT(*) AS table_count FROM dbc.tables WHERE tablekind = 'T' AND databasename = 'Test'")

table_count = int(cursor.fetchone()[0])
print(type(table_count))
print(table_count)

list_table_df = teradataml.DataFrame.from_query(
    "SELECT  TableName FROM  DBC.TablesV WHERE   TableKind = 'T' and DatabaseName = '{}'").format(database)
print(list_table_df)

table_df = list_table_df.to_pandas()  # convert to pandas DataFrame
list_table = table_df["TableName"].tolist()  # extract column as list
print(list_table)

def fetch_all_data(list_table_batch):
    for table_name in list_table_batch:
        fetch_data(table_name)


def fetch_data(table_name):
    try:
        print(f"----------------processing {table_name}")
        query = "select * from Test.{} ;".format(table_name)
        print(query)
        df = teradataml.DataFrame.from_query(query)
        df = DataFrame(table_name)
        out = fastexport(df)
        print(out)
        print("done")

        csv_data = out.to_csv(index=False)
        # Create a client to interact with the storage service
        client = storage.Client(project=project_id)
        # Upload the CSV data to the bucket

        filename = table_name + ".csv"
        blob = client.bucket(bucket_name).blob(filename)
        blob.upload_from_string(csv_data)

        print(f"DataFrame uploaded to {bucket_name}")

        # Get a reference to the CSV file in GCS
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(filename)

        # Load the data from the CSV file into a BigQuery table
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Infer schema from the CSV file
            skip_leading_rows=1,  # Skip the CSV header row
            source_format=bigquery.SourceFormat.CSV,
        )
        job = bigquery_client.load_table_from_uri(
            f"gs://{bucket_name}/{filename}", f"{dataset_id}.{table_name}", job_config=job_config
        )

        # Wait for the job to complete
        job.result()

        # Check the status of the job
        if job.errors:
            raise Exception(job.errors)

        print(f"Data loaded into {dataset_id}.{table_name} successfully.")

    except UdaExecError:
        print("Error occurred while creating context or retrieving data from Teradata")


def process_table(table_df):
    for table in table_df:
        print(table,"------------------------ process_table ------------------------------")
        fetch_data(table)
threads = []
batch_size = 70

start_time = time.time()
for i in range(0, len(list_table), batch_size):
    batch_tables = list_table[i:i+batch_size]
    print("-----------------------------{batch_tables}------------------------------------")
    thread = threading.Thread(target=process_table, args=[batch_tables])
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

end_time = time.time()
print("Time taken:", end_time - start_time, "seconds")

