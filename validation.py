"""CLI validations for BigQuery Batch SQL Translator"""

import argparse
import pathlib
import shutil


def validated_file(unvalidated_path: str) -> str:
    """Validates a path is a regular file that exists.
    Args:
        unvalidated_path: A string representing the path to validate.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path is not a regular file that exists.
    """
    path = pathlib.Path(unvalidated_path)
    if path.is_file():
        return path.as_posix()
    raise argparse.ArgumentTypeError(
        f"{path.as_posix()} is not a regular file that exists."
    )


def validated_directory(unvalidated_path: str) -> str:
    """Validates a path is a directory that exists.
    Args:
        unvalidated_path: A string representing the path to validate.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path is not a directory that exists.
    """
    path = pathlib.Path(unvalidated_path)
    if path.is_dir():
        return path.as_posix()
    raise argparse.ArgumentTypeError(
        f"{path.as_posix()} is not a directory that exists."
    )


def validated_nonexistent_path(unvalidated_path: str, force: bool = False) -> str:
    """Validates a path does not exist.
    Args:
        unvalidated_path: A string representing the path to validate.
        force: A boolean representing whether to remove unvalidated_path if it exists.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path already exists.
    """
    path = pathlib.Path(unvalidated_path)

    if not path.exists():
        return path.as_posix()

    if force:
        if path.is_dir():
            shutil.rmtree(path)
        if path.is_file():
            path.unlink()
        return path.as_posix()

    raise argparse.ArgumentTypeError(f"{path.as_posix()} already exists.")
