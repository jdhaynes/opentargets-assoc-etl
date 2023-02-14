import itertools
import os
from multiprocessing import Pool

import numpy as np

import pandas as pd


class AssociationsDataSet:
    def __init__(self, assoc_data: pd.DataFrame, target_data: pd.DataFrame,
                 disease_data: pd.DataFrame, processes) -> None:
        """
        Instantiates a new object representing an abstraction of the disease-target association dataset.
        :param assoc_data: A DateFrame containing disease-target association data.
        :param target_data: A DateFrame containing disease data.
        :param disease_data: A DateFrame containing target  data.
        """
        self.processes = processes
        self.assoc_data = assoc_data
        self.target_data = target_data
        self.disease_data = disease_data
        self.metrics = None
        self.true_assocs = None
        self.target_pairs = None

    def target_pair_has_common_diseases(self, target1: str, target2: str) -> int:
        """
        Determines if a target pair has at least 2 shared diseases.
        :param target1: ID of target 1.
        :param target2: ID of target 2.
        :return: True if target pair has at least 2 shared diseases.
        """
        target1_diseases = self.true_assocs[self.true_assocs['targetId'] == target1]['diseaseId']
        target2_diseases = self.true_assocs[self.true_assocs['targetId'] == target2]['diseaseId']
        common_diseases = np.intersect1d(target1_diseases, target2_diseases)

        return len(common_diseases) >= 2

    def count_targets_common_disease(self) -> None:
        """
        Counts the number of target pairs that share at least 2 diseases.
        """
        print(f'Computing target pair common diseases')

        # Making a naive assumption that a "true" assoc must have a median score > 0. Some caveats to consider here.
        self.true_assocs = self.metrics[self.metrics['median'] > 0]

        # Generate list of each possible target-target pair combination
        target_ids = self.assoc_data['targetId'].unique()
        target_pairs = list(itertools.combinations(target_ids, r=2))

        with Pool(self.processes) as pool:
            pair_has_common_diseases = pool.starmap(self.target_pair_has_common_diseases, target_pairs)

        self.target_pairs = sum(pair_has_common_diseases)

    def compute_score_metrics(self):
        """
        Computes the median score and top 3 score for each disease-target association pair.
        """
        print(f'Computing score metrics')

        self.metrics = self.assoc_data \
            .groupby(['targetId', 'diseaseId'])['score'] \
            .agg(median=lambda x: x.median(),
                 top3=lambda x: list(x.sort_values(ascending=False).head(3)))

    def join(self) -> None:
        """
        Joins the disease-target association data with disease and target metadata.
        """
        print(f'Joining disease and target data')

        diseases = self.disease_data.set_index('id')
        targets = self.target_data.set_index('id')

        self.metrics = self.metrics.reset_index() \
            .merge(right=targets['approvedSymbol'],
                   left_on='targetId',
                   right_on='id') \
            .merge(right=diseases['name'],
                   left_on='diseaseId',
                   right_on='id')

    def transform(self) -> None:
        """
        Performs all transformation steps on the dataset.
        """
        self.compute_score_metrics()
        self.join()
        self.count_targets_common_disease()

    def save(self, dir: str) -> None:
        """
        Writes the output data to a directory on the local filesystem.
        :param filepath: Full filepath on the local filesystem to write to.
        """
        with open(os.path.join(dir, 'output.json'), 'wb') as file:
            self.metrics.to_json(file, orient='records')

        with open(os.path.join(dir, 'output2.txt'), 'wb') as file:
            file.write(self.target_pairs)
