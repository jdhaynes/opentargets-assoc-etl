import os.path

from etl import transform

transformed_data = transform.run(
    assoc_dir='../data/assocs',
    disease_dir='../data/diseases',
    target_dir='../data/targets')

print("hi")