
import argparse
import logging
import sys
from functools import partial
from typing import List
from marshmallow import ValidationError
from teradata_ddl_Extractor import *
from schema_evolution import *
import batch_sql_translator
from config import parse as parse_config
from gcloud_auth_helper import validate_gcloud_auth_settings
from macro_processor import MacroProcessor
from object_name_mapping import parse as parse_object_name_mapping
from validation import (
    validated_directory,
    validated_file,
    validated_nonexistent_path,
)


def start_translation(args: argparse.Namespace) -> None:
    """Starts a batch sql translation job."""
    try:
        config = parse_config(args.config)
    except ValidationError:
        sys.exit(1)

    if args.object_name_mapping:
        try:
            object_name_mapping_list = parse_object_name_mapping(
                args.object_name_mapping
            )
        except ValidationError:
            sys.exit(1)
    else:
        object_name_mapping_list = None

    if args.macros:
        try:
            preprocessor = MacroProcessor(args)
        except ValidationError:
            sys.exit(1)
    else:
        preprocessor = None



    logging.info("Verify cloud login and credential settings...")
    validate_gcloud_auth_settings(config.gcp_settings.project_number)


    translator = batch_sql_translator.BatchSqlTranslator(
        config, args.input, args.output, preprocessor, object_name_mapping_list
    )
    translator.start_translation()


def parse_args(args: List[str]) -> argparse.Namespace:
    """Argument parser for the BigQuery Batch SQL Translator CLI."""
    parser = argparse.ArgumentParser(
        description="Config the Batch Sql translation tool."
    )
    parser.add_argument(
        "--verbose", help="Increase output verbosity", action="store_true"
    )
    parser.add_argument(
        "--config",
        type=validated_file,
        default="config.yaml",
        help="Path to the config.yaml file. (default: config.yaml)",
    )
    parser.add_argument(
        "--input",
        type=validated_directory,
        default="Source_DDL",
        help="Path to the input_directory. (default: Source_DDL)",
    )
    parser.add_argument(
        "--output",
        type=partial(validated_nonexistent_path, force=True),
        default="BigQuery_Compatible_DDL",
        help="Path to the output_directory. (default: BigQuery_Compatible_DDL)",
    )

    parser.add_argument(
        "-m",
        "--macros",
        type=validated_file,
        help="Path to the macro map yaml file. If specified, the program will "
        "pre-process all the input query files by replacing the macros with "
        "corresponding string values according to the macro map definition. After "
        "translation, the program will revert the substitutions for all the output "
        "query files in a post-processing step.  The replacement does not apply for "
        "files with extension of .zip, .csv, .json.",
    )
    parser.add_argument(
        "-o",
        "--object_name_mapping",
        type=validated_file,
        help="Path to the object name mapping json file. Name mapping lets you "
        "identify the names of SQL objects in your source files, and specify target "
        "names for those objects in BigQuery. More info please see "
        "https://cloud.google.com/bigquery/docs/output-name-mapping.",
    )
    parser.add_argument("--source", help="Source Database name")

    parsed_args = parser.parse_args(args)

    logging.basicConfig(
        level=logging.DEBUG if parsed_args.verbose else logging.INFO,
        format="%(asctime)s: %(levelname)s: %(message)s",
    )

    return parsed_args



def main() -> None:
    """CLI for BigQuery Batch SQL Translator"""
    args = parse_args(sys.argv[1:])
    return start_translation(args)


if __name__ == "__main__":
    main()