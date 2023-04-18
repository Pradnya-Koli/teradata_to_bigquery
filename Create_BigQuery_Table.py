from google.api_core.exceptions import NotFound
from google.cloud import bigquery


# service_account_file_path = "searce-practice-data-analytics-a25961da78a0.json"
# client = bigquery.Client.from_service_account_json(service_account_file_path)


def create_bq_dataset(dataset_list):
    # Construct a BigQuery client object.
    project = 'searce-practice-data-analytics'
    client = bigquery.Client(project=project)
    #    client = bigquery.Client.from_service_account_json(service_account_file_path)
    dataset_id = "{}.{}".format(project, dataset_list)
    print(dataset_id)
    dataset = bigquery.Dataset(dataset_id)
    try:
       client.get_dataset(dataset_list)  # Make an API request.
       print("Dataset  already exists")
    except NotFound:
          dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
          print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def create_bq_table():
    project = 'searce-practice-data-analytics'
    client = bigquery.Client(project=project)

    text_file = open("BigQuery_Compatible_DDL/source_ddl.sql", "r")

    # read whole file to a string
    data = text_file.read()
    ddl_list = data.split(";")
    print(ddl_list)

    # close file
    text_file.close()

    for item in ddl_list[:-1]:
        sql = """{} {}""".format(item, ";")
        print(sql)
        job = client.query(sql)  # API request.
        job.result()  # Waits for the query to finish.
        print("Successfully Created")
