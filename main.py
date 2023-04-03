import argparse
import time
from sql_translator_main import *
from Create_BigQuery_Table import *
from teradata_ddl_Extractor import *
from schema_evolution import *
if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(description="""
    Source Database for Schema Conversion
    """)
    parser.add_argument("--source", help="Source Database name")

    args = parser.parse_args()

    Source = args.source

    if Source == "schema_evolution":
        schema_evolution()
        main()
        print("Schema Migrated Successfully")

    if Source == "teradata":
        teradata_ddl_extractor()
        main()
        create_bq_table()
        print("Schema Migrated Successfully")
    end = time.time()
    print("Total time taken :" ,(end-start))

    print("Thank You for using Generic Schema Convertor")

