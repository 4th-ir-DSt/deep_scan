
from __future__ import print_function
import os
import sys
import argparse
import boto3


def upload_to_s3(artefacts_file, bucket_name):
    """
    Uploads an artefact to Amazon S3
    """
    s3_resource = boto3.resource('s3')
    with open(artefacts_file, "r", encoding='utf-8') as artefacts:
        lines = artefacts.read().splitlines()
        for line in lines:
            artefact=line.split(',')[0]
            bucket_key=line.split(',')[1]
            try:
                s3_resource.meta.client.upload_file(
                    '{0}/{1}'.format(os.getcwd(), artefact), bucket_name, '{0}'.format(bucket_key)
                )
            except IOError as err:
                print("Failed to access artefact in this directory.\n" + str(err))
                return False
        return True


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("bucket_name", help="Solution Config Bucket")
    parser.add_argument("artefacts_file", help="Name of the artefacts file")
    args = parser.parse_args()

    if not upload_to_s3(args.artefacts_file, args.bucket_name ):
        sys.exit(1)


if __name__ == "__main__":
    main()
