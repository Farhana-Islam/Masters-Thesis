"""
This module provides a utility function for reading SQL query files.

Functions:
    get_query_by_name(name: str) -> str:
        Reads a SQL query from a file in the same directory as this module.

        Args:
            name (str): The name of the SQL file to read, without the .sql extension.

        Returns:
            str: The content of the SQL file as a string.

        Raises:
            FileNotFoundError: If the specified SQL file does not exist in the current directory.
"""

import os


def get_query_by_name(name: str) -> str:
    """
    Read a SQL query from a file.

    Args:
        name (str): The name of the SQL file to read, without the .sql extension.

    Returns:
        str: The content of the SQL file as a string.

    Raises:
        FileNotFoundError: If the specified SQL file does not exist.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, f"{name}.sql")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The SQL file {file_path} could not be found.")

    with open(file_path, "r") as f:
        content = f.read()

    return content
