import json

import os
from os.path import splitext

import pandas as pd


class JSONFileDirectory:
    def __init__(self, dir: str) -> None:
        """
        Instantiates a new object representing a JSON file on the local filesystem.
        :param dir:
        """
        self.dir = dir

    def to_dataframe(self, columns: list[str]) -> pd.DataFrame:
        """
        Generates a DataFrame of data from the JSON file with the columns specified.
        :param columns: A list of JSON field names to extract as DataFrame columns.
        :return: A DataFrame representation of the JSON file.
        """
        dataframes = []

        files = os.listdir(self.dir)
        for file in files:
            filepath = os.path.join(self.dir, file)
            filename, extension = splitext(filepath)

            # Only load data from JSON files, to exclude "_SUCCESS" files in EMBL-EBI FTP dirs.
            if extension == ".json":
                json_file = JSONFile(filepath)
                dataframe = json_file.to_dataframe(columns)
                dataframes.append(dataframe)

        output = pd.concat(dataframes)
        return output


class JSONFile:
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def to_json(self):
        with open(self.filepath, 'r') as f:
            body = f.read()

        return body

    def to_dataframe(self, columns: list[str]) -> pd.DataFrame:
        json_body = self.to_json()
        assocs_list = json_body.strip().split('\n')
        assocs = []
        for assoc in assocs_list:
            json_obj = json.loads(assoc)
            data = [json_obj[x] for x in columns]
            assocs.append(data)

            df = pd.DataFrame(data=assocs, columns=columns)

        return df
