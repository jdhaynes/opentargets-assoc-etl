import json

import os
from multiprocessing import Pool
from os.path import splitext

import pandas as pd


class JSONFileDirectory:
    def __init__(self, dir: str) -> None:
        """
        Instantiates a new object representing a JSON file on the local filesystem.
        :param dir:
        """
        self.dir = dir

    def list_dir(self):
        filenames = os.listdir(self.dir)
        filepaths = []

        for f in filenames:
            filepath = os.path.join(self.dir, f)
            filename, extension = splitext(filepath)

            if extension == '.json':
                filepaths.append(filepath)

        return filepaths

    def to_dataframe(self, columns: list[str], processes: int) -> pd.DataFrame:
        """
        Generates a DataFrame of data from the JSON file with the columns specified.
        :param processes: Number of processes to use for multiprocessing.
        :param columns: A list of JSON field names to extract as DataFrame columns.
        :return: A DataFrame representation of the JSON file.
        """
        filepaths = self.list_dir()
        files = [JSONFile(f) for f in filepaths]

        # Parse each JSON file to DateFrame in parallel using multiprocessing, as this operation
        # is the most CPU-intensive.
        with Pool(processes) as pool:
            dataframes = pool.starmap(JSONFile.to_dataframe, [(f, columns) for f in files])

        # Merge the rows of each individual DataFrame (for each file) into one - this is quick
        # and doesn't need multiprocessing.
        output = pd.concat(dataframes)

        return output


class JSONFile:
    def __init__(self, filepath: str) -> None:
        """
        Instantiates an object representing a JSON file on the local filesystem.
        :param filepath: Full file path to the JSON file.
        """
        self.filepath = filepath

    def to_json(self) -> str:
        """
        Gets JSON string representing the content of the file.
        :return: JSON string representing the content of the file.
        """
        with open(self.filepath, 'r') as f:
            body = f.read()

        return body

    def to_dataframe(self, columns: list[str]) -> pd.DataFrame:
        """
        Gets DataFrame of the JSON data. Only the columns specified are included.
        :param columns: List of fields to use as columns. All other fields are discarded.
        :return: A DataFrame representation of the JSON data.
        """
        print(f'Parsing JSON->DF for {self.filepath}')

        json_body = self.to_json()
        assocs_list = json_body.strip().split('\n')
        assocs = []
        for assoc in assocs_list:
            json_obj = json.loads(assoc)
            data = [json_obj[x] for x in columns]
            assocs.append(data)

        df = pd.DataFrame(data=assocs, columns=columns)

        return df

