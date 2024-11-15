"""Global variables.

Author: Gaurav Thagunna
Date : 2024-11-08
Description : This module is used as global variables.
"""

import json
import os

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_file_path = os.path.join(root_dir, "config", "config.json")


class Variables:
    """Variables can be accessed.

    It stores variables parsed from the confguration.
    """

    def __init__(self, usecase_id=1):
        """Initialize object."""

        # Configuration file path

        # Reading configuration
        with open(config_file_path, "r") as file:
            self.config = json.load(file)

    def __getitem__(self, key):
        """Get the value from key."""
        return self.config[key]
