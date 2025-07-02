###################################################
##  Auther      : Waheeb Agherdien
##  Created     : 3 September 2022
##  Description : Create SRF json file from System Config
##                and SRT
##-------------------------------------------------
##  Ammended by :
##  Date        :
##  Reason      :
###################################################
##
import argparse
import json

##  Functin to retrieve passing parameters
def get_input():
    parser = argparse.ArgumentParser(description='CREATE SRF')
    parser.add_argument(
        '--sysconfig', required=True, help=("path to System Config")
    )
    parser.add_argument(
        '--template', required=True, help=("path to srf template")
    )
    parser.add_argument(
        '--run_id', required=True, help=("run id")
    )
    parser.add_argument(
        '--run_date', required=True, help=("run date")
    )
    parser.add_argument(
        '--assumed_transaction_date', required=True, help=("assumed transaction date")
    )
    parser.add_argument(
        '--output_srf_location', required=True, help=("location of srf")
    )
    args = parser.parse_args()
    return args.sysconfig, args.template, args.run_id, args.run_date, args.assumed_transaction_date, args.output_srf_location

## Main Code Block
##
sysconfig,template, run_id, run_date, assumed_transaction_date, output_srf_location = get_input()

print (sysconfig)

# Load System Config
with open(sysconfig) as s:
    sysdata = json.load(s)

print (template)

# Load SRT
with open(template) as t:
    srtdata = json.load(t)

srtdata["run_id"] = run_id
srtdata["run_date"] = run_date
srtdata["assumed_transaction_date"] = assumed_transaction_date

# Set System Data, buckets etc.
srtdata['s3_buckets']['logs_bucket'] = sysdata['s3_buckets']['logs_bucket']
srtdata['s3_buckets']['working_bucket'] = sysdata['s3_buckets']['working_bucket']
srtdata['s3_buckets']['landing_bucket'] = sysdata['s3_buckets']['landing_bucket']
srtdata['s3_buckets']['landing_archive_bucket'] = sysdata['s3_buckets']['landing_archive_bucket']
srtdata['s3_buckets']['source_bucket'] = sysdata['s3_buckets']['source_bucket']
srtdata['s3_buckets']['validation_bucket'] = sysdata['s3_buckets']['validation_bucket']
srtdata['s3_buckets']['solution_config_bucket'] = sysdata['s3_buckets']['solution_config_bucket']
#srtdata['s3_buckets']['dl_landing_bucket'] = sysdata['s3_buckets']['dl_landing_bucket']
srtdata['s3_buckets']['vault_bucket'] = sysdata['s3_buckets']['vault_bucket']
srtdata['s3_buckets']['webservice_bucket'] = sysdata['s3_buckets']['webservice_bucket']

#Write the SRF for use by the job
with open(output_srf_location, 'w') as jsonfile:
    json.dump(srtdata, jsonfile)
