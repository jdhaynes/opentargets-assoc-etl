import os

from etl.data import AssociationsDataSet
from etl.ftp import FTPBatchDownloader
from etl.json import JSONFileDirectory


class Pipeline:
    def __init__(self, server: str, assoc_dir: str, target_dir: str,
                 disease_dir: str, output_dir: str, processes: int) -> None:
        self.server = server
        self.processes = processes

        self.dirs = {'assoc': assoc_dir,
                     'target': target_dir,
                     'disease': disease_dir,
                     'output': output_dir}

        self.data = {}
        self.analysis = None

    def download_data(self):
        datasets = ['assoc', 'target', 'disease']
        for d in datasets:
            # Create the directory.
            target_dir = os.path.join(self.dirs['output'], d)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)

            downloader = FTPBatchDownloader()
            downloader.download_dir(server_dir=self.dirs['target'],
                                    output_dir=self.dirs['output'])

    def load_data(self):
        self.data['assoc'] = JSONFileDirectory(self.dirs['assoc'])
        self.data['target'] = JSONFileDirectory(self.dirs['target'])
        self.data['disease'] = JSONFileDirectory(self.dirs['disease'])

        self.analysis = AssociationsDataSet(assoc_data=self.data['assoc'],
                                            target_data=self.data['target'],
                                            disease_data=self.data['disease'])

    def run(self):
        self.download_data()
        self.load_data()
        self.analysis.transform()
        self.analysis.save()
