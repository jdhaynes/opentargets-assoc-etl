import pandas as pd

from etl import transform


def test_score_median_correct():
    input_data = pd.DataFrame(data={'targetId': ['target1', 'target1', 'target2', 'target2', 'target2'],
                                    'diseaseId': ['disease1', 'disease1', 'disease2', 'disease2', 'disease2'],
                                    'score': [3, 4, 6, 6, 7]},
                              index=['targetId', 'diseaseId'])

    expected_data = pd.DataFrame(data={'targetId': ['target1', 'target2'],
                                       'diseaseId': ['disease1', 'disease2'],
                                       'median': [3.5, 6]},
                                 index=['targetId', 'diseaseId'])

    output_data = transform.compute_score_metrics(input_data)
    output_data = output_data['median']

    pd.testing.assert_frame_equal(input_data, output_data)

def test():
    transformed_data = transform.run(assoc_dir='../data/assoc',
                                     disease_dir='../data/disease',
                                     target_dir='../data/target')