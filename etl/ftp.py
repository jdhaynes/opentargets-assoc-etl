import os
from ftplib import FTP
from multiprocessing import Pool


class FTPConnection:
    def __init__(self, server: str) -> None:
        """
        Instantiates a new connection object to an FTP server.
        :param server: The address of the FTP server.
        """
        self.server = server
        self.conn = FTP(self.server)
        self.conn.login()

    def __del__(self) -> None:
        """
        Deconstructor to ensure server connection resources are closed on object garbage collection.
        """
        self.conn.close()

    def download_file(self, server_dir: str, server_filename: str, output_dir: str) -> None:
        """
        Downloads a file from the FTP server to the local filesystem.
        :param server_dir: Directory on the FTP server containing the file.
        :param server_filename: Name of file to download.
        :param output_dir: Directory on local filesystem to download the file to.
        """
        local_filepath = os.path.join(output_dir, server_filename)
        server_filepath = os.path.join(self.server, server_dir, server_filename)

        self.conn.cwd(server_dir)

        with open(local_filepath, 'wb') as file:
            print(f'Starting file download {server_filepath}')
            self.conn.retrbinary(f'RETR {server_filename}', file.write)
            print(f'Finished file download {server_filepath}')

    def list_dir(self, dir: str) -> list[str]:
        """
        Generates list of files in a specified directory on FTP server.
        :param dir: Directory name.
        :return: A list of files in the specified directory.
        """
        self.conn.cwd(dir)
        return self.conn.nlst()


class FTPBatchDownloader:
    def __init__(self, server: str) -> None:
        """
        Instantiates a new object responsible for downloading multiple files in parallel from an FTP server.
        :param server: The address of the FTP server.
        """
        self.server = server

    def download_file(self, server_dir: str, filename: str, output_dir: str) -> None:
        """
        Downloads a single file from the FTP server to the local filesystem.
        :param server_dir: Directory on the FTP server containing the file.
        :param filename: Name of file within server_dir to download.
        :param output_dir: Directory on local filesystem to download the file to.
        """
        ftp = FTPConnection(self.server)
        ftp.download_file(server_dir=server_dir,
                          server_filename=filename,
                          output_dir=output_dir)

    def download_dir(self, server_dir: str, output_dir: str, processes: int) -> None:
        """
        Downloads all files from the specified directory on the FTP server to the local filesystem.
        :param server_dir: Directory on the FTP server to download the contents of.
        :param output_dir: Directory on local filesystem to download the directory contents to.
        :param processes: Number of files to simultaneously download.
        """
        files = FTPConnection(self.server).list_dir(server_dir)
        # Use multiprocessing to download multiple files in parallel.
        with Pool(processes) as pool:
            pool.starmap(self.download_file, [(server_dir, x, output_dir) for x in files])