import os
from ftplib import FTP
from multiprocessing import Pool


def download_file(filename: str, server: str, dir: str, output_dir: str) -> None:
    """
    Downloads a single file from an FTP server to the local filesystem.
    :param filename: The filename (excluding path) of the file to download.
    :param server: The FTP server address.
    :param dir: The directory containing the file on the FTP server.
    :param output_dir: The directory on the local filesystem to download the file to.
    """
    with FTP(server) as ftp_conn:
        ftp_conn.login()
        ftp_conn.cwd(dir)

        local_filepath = os.path.join(output_dir, filename)
        server_filepath = os.path.join(server, dir, filename)
        with open(local_filepath, 'wb') as downloaded_file:
            print(f'EXTRACT | Starting file download {server_filepath}')
            ftp_conn.retrbinary(f'RETR {filename}', downloaded_file.write)
            print(f'EXTRACT | Finished file download {server_filepath}')


def download_dir(server: str, ftp_dir: str, output_dir: str, processes: int) -> None:
    """
    Downloads all files from a given directory of an FTP server to local filesystem.
    :param processes: Number of processes to use for multiprocessing of file downloads.
    :param server: The network address of the FTP server.
    :param ftp_dir: The directory on the FTP server to download files from.
    :param output_dir: The directory on the local filesystem to download files to.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    ftp_files = []
    with FTP(server) as ftp_conn:
        ftp_conn.login()
        ftp_conn.cwd(ftp_dir)
        ftp_files = ftp_conn.nlst()  # Gets list of all files in the FTP directory

    # Download each file in the directory. These operations are sped up by using the multiprocessing library
    # to parallelize the download operations.
    with Pool(processes) as pool:
        pool.starmap(download_file, [(x, server, ftp_dir, output_dir) for x in ftp_files])


def run(ftp_server: str, assoc_dir: str, target_dir: str, disease_dir: str,
        output_dir: str, processes: int) -> None:
    """
    Runs the extract module by downloading the datasets from the FTP server.
    :param processes: Number of processes to use for multiprocessing of file downloads.
    :param ftp_server: The network address of the FTP server.
    :param assoc_dir: The directory on the FTP server containing the target-disease association dataset.
    :param target_dir: The directory on the FTP server containing the target dataset.
    :param disease_dir: The directory on the FTP server containing the disease dataset.
    :param output_dir: The directory on the local filesystem to download files to. Subdirectories will be created for each dataset.
    """
    download_dir(ftp_server, assoc_dir, os.path.join(output_dir, 'assocs'), processes)
    download_dir(ftp_server, target_dir, os.path.join(output_dir, 'targets'), processes)
    download_dir(ftp_server, disease_dir, os.path.join(output_dir, 'diseases'), processes)
