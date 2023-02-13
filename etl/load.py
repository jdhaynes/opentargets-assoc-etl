import os
import pandas as pd


def save_to_json(data: pd.DataFrame, output_path: str) -> None:
    """
    Writes the contents of a DataFrame object to a JSON file on the local filesystem.
    :param data: A DataeFrame to save as JSON.
    :param output_path: The full filepath (incl. filename) to write the JSON data to on the local filesystem.
    """
    print(f'LOAD | Saving data to JSON file at {output_path}')
    with open(output_path, 'wb') as file:
        data.to_json(file, orient='records')


def run(data: pd.DataFrame, output_dir: str) -> None:
    """
    Runs the load module by saving the transformed data to JSON format on the local filesystem.
    :param data: A DataeFrame to save as JSON.
    :param output_dir: The full filepath (incl. filename) to write the JSON data to on the local filesystem.
    """
    output_df = data.sort_values('median')
    save_to_json(data=output_df,
                 output_path=os.path.join(output_dir, 'output.json'))
