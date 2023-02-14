import pandas as pd


class AssociationsDataSet:
    def __init__(self, assoc_data: pd.DataFrame, target_data: pd.DataFrame,
                 disease_data: pd.DataFrame) -> None:
        self.assoc_data = assoc_data
        self.target_data = target_data
        self.disease_data = disease_data
        self.metrics = None

    def count_target_pairs(self):
        pass

    def compute_score_metrics(self):
        self.metrics = self.assoc_data \
            .groupby(['targetId', 'diseaseId'])['score'] \
            .agg(median=lambda x: x.median(),
                 top3=lambda x: list(x.sort_values(ascending=False).head(3)))

    def join(self) -> None:
        diseases = self.disease_data.set_index('id')
        targets = self.target_data.set_index('id')

        self.metrics = self.metrics.reset_index() \
            .merge(right=targets['approvedSymbol'],
                   left_on='targetId',
                   right_on='id') \
            .merge(right=diseases['name'],
                   left_on='diseaseId',
                   right_on='id')

    def transform(self, ) -> None:
        self.compute_score_metrics()
        self.join()

    def save(self, filepath: str) -> None:
        with open(filepath, 'wb') as file:
            self.metrics.to_json(file, orient='records')
