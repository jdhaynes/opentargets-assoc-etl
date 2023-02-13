import json
import os
from os.path import splitext

import pandas as pd


def json_to_df(body: str, columns: list[str]) -> pd.DataFrame:
    """
    Converts a raw JSON body into a pandas DataFrame containing only the diseaseId, targetId and score fields.
    :param columns: List of columns to include in the DataFrame. This is for performance results to avoid loading of unused fields.
    :param body: String representing the raw JSON body.
    :return: A pandas DataFrame containing only the diseaseId, targetId and score fields.
    """
    # The JSON files contain multiple JSON objects (each representing an association) per file, seperated by a new line.
    # This isn't recognised as valid JSON by deserialisers, so must split into separate JSON objects by new line
    # and parsed individually.
    assocs_str = body.strip().split('\n')
    assocs = []
    for assoc in assocs_str:
        json_obj = json.loads(assoc)
        data = [json_obj[x] for x in columns]
        assocs.append(data)

    df = pd.DataFrame(data=assocs, columns=columns)
    return df


def load_df_from_json_file(filepath: str, columns: list[str]) -> pd.DataFrame:
    with open(filepath, 'r') as f:
        body = f.read()
        print(f'TRANSFORM | Parsing JSON to DF for local file {filepath}')
        df = json_to_df(body, columns)

    return df


def load_df_from_dir(dir: str, columns: list[str]) -> pd.DataFrame:
    """
    Loads JSON data from the given directory into a DataFrame, extracting only the columns specified.
    :rtype: object
    :param dir: The directory containing the JSON files. Multiple files will be merged into a single DataFrame by row.
    :param columns: A list of the fields to be extracted from the JSON file and turned into DataFrame columns.
    :return: A DataFrame representing the loaded JSON data.
    """
    dfs = []

    files = os.listdir(dir)
    for file in files:
        full_filename = os.path.join(dir, file)
        filename, extension = splitext(full_filename)

        # Only load data from JSON files, to exclude "_SUCCESS" files in EMBL-EBI FTP dirs.
        if extension == ".json":
            df = load_df_from_json_file(os.path.join(dir, file), columns)
            dfs.append(df)

    print(f'TRANSFORM | Merging individual DFs into single DF')
    data = pd.concat(dfs)
    return data


def compute_score_metrics(data: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the median and top 3 scores for each pair of target and disease.
    :param data: A DataFrame containing the target-disease association data.
    :return: A DataFrame containing the median and top 3 scores for each pair of target and disease.
    """
    print(f'TRANSFORM | Computing median and top 3 score metrics')

    score_metrics = data \
        .groupby(['targetId', 'diseaseId'])['score'] \
        .agg(median=lambda x: x.median(),
             top3=lambda x: list(x.sort_values(ascending=False).head(3)))

    return score_metrics


def join(assocs: pd.DataFrame, targets: pd.DataFrame, diseases: pd.DataFrame) -> pd.DataFrame:
    """
    Joins the target-disease association dataset with the target and disease information.
    :param assocs: A DataFrame containing the target-disease dataset.
    :param targets: A DataFrame containing the target dataset.
    :param diseases: A DataFrame containing the disease dataset.
    :return: A DataFrame containing the merged dataset.
    """
    print(f'TRANSFORM | Joining associations with disease and target data')

    diseases = diseases.set_index('id')
    targets = targets.set_index('id')
    joined = assocs.reset_index() \
        .merge(right=targets['approvedSymbol'],
               left_on='targetId',
               right_on='id') \
        .merge(right=diseases['name'],
               left_on='diseaseId',
               right_on='id')

    return joined


def count_target_pairs(assocs: pd.DataFrame) -> int:
    df = assocs[assocs['score'] > 0]  # Assume that assocs with score 0 are false, so remove them.
    df = df.reset_index()

    output = df.merge(right=df[['targetId', 'diseaseId']], how='cross', on='targetId')
    print('hi')


def run(assoc_dir: str, target_dir: str, disease_dir: str) -> pd.DataFrame:
    """
    Runs the transform module on JSON data downloaded in the extract module.
    :rtype: object
    :param assoc_dir: Directory containing downloaded target-disease association JSON data (from extract module).
    :param target_dir: Directory containing downloaded target JSON data (from extract module).
    :param disease_dir: Directory containing downloaded disease association JSON data (from extract module).
    :return: A DataFrame containing the transformed dataset.
    """
    assocs_df = load_df_from_dir(dir=assoc_dir, columns=['targetId', 'diseaseId', 'score'])
    target_df = load_df_from_dir(dir=target_dir, columns=['id', 'approvedSymbol'])
    disease_df = load_df_from_dir(dir=disease_dir, columns=['id', 'name'])

    output_df = compute_score_metrics(assocs_df)
    output_df = join(output_df, target_df, disease_df)

    return output_df
