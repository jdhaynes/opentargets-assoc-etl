import os

from datasets import AssociationsDataSet
from ftp import FTPBatchDownloader
from json_data import JSONFileDirectory


class Pipeline:
    def __init__(self, server: str, evidence_dir: str, target_dir: str,
                 disease_dir: str, output_dir: str, processes: int) -> None:
        """
        Creates a new pipeline instance.
        :param server: The FTP server address containing the data.
        :param evidence_dir: Directory on the FTP server containing the evidence data.
        :param target_dir: Directory on the FTP server containing the target data.
        :param disease_dir: Directory on the FTP server containing the disease data.
        :param output_dir: Directory on the local filesystem to write pipeline output data to.
        :param processes: Number of processes used in tasks where multiprocessing is utilised.
        """
        self.server = server
        self.processes = processes

        self.output_root = output_dir
        self.server_dirs = {'evidence': evidence_dir,
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
        self.data['evidence'] = JSONFileDirectory(self.output_dirs['evidence']) \
            .to_dataframe(columns=['targetId', 'diseaseId', 'score'],
                          processes=self.processes)

        self.data['target'] = JSONFileDirectory(self.output_dirs['target']) \
            .to_dataframe(columns=['id', 'approvedSymbol'],
                          processes=self.processes)

        self.data['disease'] = JSONFileDirectory(self.output_dirs['disease']) \
            .to_dataframe(columns=['id', 'name'],
                          processes=self.processes)

        self.analysis = AssociationsDataSet(evidence_data=self.data['evidence'],
                                            target_data=self.data['target'],
                                            disease_data=self.data['disease'],
                                            processes=self.processes)

    def run(self) -> None:
        """
        Runs the full pipeline.
        """
        self.download_data()
        self.load_data()
        self.analysis.transform()
        self.analysis.save(dir=self.output_root)
