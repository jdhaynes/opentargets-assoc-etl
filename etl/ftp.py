import os
from ftplib import FTP
from multiprocessing import Pool


class FTPConnection:
    def __init__(self, server: str) -> None:
        self.server = server
        self.conn = FTP(self.server)
        self.conn.login()

    def __del__(self) -> None:
        self.conn.close()

    def download_file(self, server_dir: str, server_filename: str, output_dir: str) -> None:
        local_filepath = os.path.join(output_dir, server_filename)
        server_filepath = os.path.join(self.server, server_dir, server_filename)

        self.conn.cwd(server_dir)

        with open(local_filepath, 'wb') as file:
            print(f'Starting file download {server_filepath}')
            self.conn.retrbinary(f'RETR {server_filename}', file.write)
            print(f'Finished file download {server_filepath}')

    def list_dir(self, dir: str) -> list[str]:
        self.conn.cwd(dir)
        return self.conn.nlst()


class FTPBatchDownloader:
    def __init__(self, server: str) -> None:
        self.server = server

    def download_file(self, server_dir: str, filepath: str, output_dir: str) -> None:
        ftp = FTPConnection(self.server)
        ftp.download_file(server_dir=server_dir,
                          server_filename=filepath,
                          output_dir=output_dir)

    def download_dir(self, server_dir: str, output_dir: str, processes: int) -> None:
        files = FTPConnection(self.server).list_dir(server_dir)
        with Pool(processes) as pool:
            pool.starmap(self.download_file, [(server_dir, x, output_dir) for x in files])