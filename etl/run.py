import argparse
import os

import extract
import load
import transform

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--ftp-server', required=True)
parser.add_argument('-a', '--assoc-dir', required=True)
parser.add_argument('-d', '--disease-dir', required=True)
parser.add_argument('-t', '--target-dir', required=True)
parser.add_argument('-o', '--output-dir', default='./data')
parser.add_argument('-p', '--processes', default=1, type=int)

args = parser.parse_args()

if __name__ == '__main__':
    extract.run(ftp_server=args.ftp_server,
                assoc_dir=args.assoc_dir,
                disease_dir=args.disease_dir,
                target_dir=args.target_dir,
                output_dir=args.output_dir,
                processes=args.processes)

    transformed_data = transform.run(assoc_dir=os.path.join(args.output_dir, 'assocs'),
                                     disease_dir=os.path.join(args.output_dir, 'diseases'),
                                     target_dir=os.path.join(args.output_dir, 'targets'))

    load.run(data=transformed_data,
             output_dir=args.output_dir)

