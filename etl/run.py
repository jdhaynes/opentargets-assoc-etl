import argparse

from pipeline import Pipeline

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--ftp-server', required=True)
    parser.add_argument('-a', '--assoc-dir', required=True)
    parser.add_argument('-d', '--disease-dir', required=True)
    parser.add_argument('-t', '--target-dir', required=True)
    parser.add_argument('-o', '--output-dir', default='./data')
    parser.add_argument('-p', '--processes', default=1, type=int)
    args = parser.parse_args()

    pipeline = Pipeline(server=args.ftp_server,
                        assoc_dir=args.assoc_dir,
                        disease_dir=args.disease_dir,
                        target_dir=args.target_dir,
                        output_dir=args.output_dir,
                        processes=args.processes)

    pipeline.run()
