import os

from datasets import AssociationsDataSet
from ftp import FTPBatchDownloader
from json_data import JSONFileDirectory


class Pipeline:
    def __init__(self, server: str, assoc_dir: str, target_dir: str,
                 disease_dir: str, output_dir: str, processes: int) -> None:
        """
        Creates a new pipeline instance.
        :param server: The FTP server address containing the data.
        :param assoc_dir: Directory on the FTP server containing the disease-target association data.
        :param target_dir: Directory on the FTP server containing the target data.
        :param disease_dir: Directory on the FTP server containing the disease data.
        :param output_dir: Directory on the local filesystem to write pipeline output data to.
        :param processes: Number of processes used in tasks where multiprocessing is utilised.
        """
        self.server = server
        self.processes = processes

        self.output_root = output_dir
        self.server_dirs = {'assoc': assoc_dir,
                     'target': target_dir,
                     'disease': disease_dir}
        self.output_dirs = {x: os.path.join(self.output_root, x) for x in self.server_dirs}

        self.data = {}
        self.analysis = None

    def download_data(self) -> None:
        """
        Downloads the data from the FTP server onto the local filesystem.
        """
        datasets = ['assoc', 'target', 'disease']
        for d in datasets:
            # Create the directory.
            target_dir = os.path.join(self.output_root, d)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)

            downloader = FTPBatchDownloader(server=self.server)
            downloader.download_dir(server_dir=self.server_dirs[d],
                                    output_dir=self.output_dirs[d],
                                    processes=self.processes)

    def load_data(self) -> None:
        """
        Loads the downloaded data into memory for transformation.
        """
        self.data['assoc'] = JSONFileDirectory(self.output_dirs['assoc']).to_dataframe(columns=['targetId', 'diseaseId', 'score'])
        self.data['target'] = JSONFileDirectory(self.output_dirs['target']).to_dataframe(columns=['id', 'name'])
        self.data['disease'] = JSONFileDirectory(self.output_dirs['disease']).to_dataframe(columns=['id', 'approvedSymbol'])

        self.analysis = AssociationsDataSet(assoc_data=self.data['assoc'],
                                            target_data=self.data['target'],
                                            disease_data=self.data['disease'])

    def run(self) -> None:
        """
        Runs the full pipeline.
        """
        self.download_data()
        self.load_data()
        self.analysis.transform()
        self.analysis.save_output()
