import pandas as pd

from etl.datasets import AssociationsDataSet


class TestDatasets:
    mock_assoc = pd.DataFrame(data={'targetId': ['target1', 'target1', 'target1', 'target1', 'target2'],
                                    'diseaseId': ['disease1', 'disease1', 'disease1', 'disease1', 'disease2'],
                                    'score': [3, 6, 7, 11, 15]})

    mock_target = pd.DataFrame(data={'id': ['target1', 'target2'],
                                     'approvedSymbol': ['symbol1', 'symbol2']})

    mock_disease = pd.DataFrame(data={'id': ['disease1', 'disease2'],
                                      'name': ['diseaseName1', 'diseaseName2']})

    def test_score_metrics_has_correct_median(self):
        dataset = AssociationsDataSet(evidence_data=self.mock_assoc,
                                      target_data=self.mock_target,
                                      disease_data=self.mock_disease,
                                      processes=2)

        dataset.compute_score_metrics()

        expected = pd.DataFrame(data={'targetId': ['target1', 'target2'],
                                      'diseaseId': ['disease1', 'disease2'],
                                      'median': [6.5, 15],
                                      'top3': [[11, 7, 6], [15]]}).set_index(['targetId', 'diseaseId'])

        pd.testing.assert_series_equal(left=dataset.assocs['median'], right=expected['median'])

    def test_score_metrics_has_correct_top3(self):
        dataset = AssociationsDataSet(evidence_data=self.mock_assoc,
                                      target_data=self.mock_target,
                                      disease_data=self.mock_disease,
                                      processes=2)

        dataset.compute_score_metrics()

        expected = pd.DataFrame(data={'targetId': ['target1', 'target2'],
                                      'diseaseId': ['disease1', 'disease2'],
                                      'median': [6.5, 15],
                                      'top3': [[11, 7, 6], [15]]}).set_index(['targetId', 'diseaseId'])

        pd.testing.assert_series_equal(left=dataset.assocs['top3'], right=expected['top3'])

    def test_transform_joins_correct_data(self):
        dataset = AssociationsDataSet(evidence_data=self.mock_assoc,
                                      target_data=self.mock_target,
                                      disease_data=self.mock_disease,
                                      processes=2)

        dataset.assocs = dataset.evidence_data
        dataset.join()

        expected = pd.DataFrame(data={'targetId': ['target1', 'target1', 'target1', 'target1', 'target2'],
                                      'diseaseId': ['disease1', 'disease1', 'disease1', 'disease1', 'disease2'],
                                      'score': [3, 6, 7, 11, 15],
                                      'name': ['diseaseName1', 'diseaseName1', 'diseaseName1',
                                               'diseaseName1', 'diseaseName2'],
                                      'approvedSymbol': ['symbol1', 'symbol1', 'symbol1', 'symbol1', 'symbol2']
                                      })

        pd.testing.assert_frame_equal(left=dataset.assocs[['name', 'approvedSymbol']],
                                      right=expected[['name', 'approvedSymbol']])
