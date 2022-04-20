#!/usr/bin/python
import os
import sys
import getopt
import yaml
from dateutil.relativedelta import *
import datetime as dt


def get_args(argv):
    tech_conf_file = ''
    conf_file = ''
    scope = ''
    try:
        opts, args = getopt.getopt(argv, "ht:s:c:", ["techconf=", "conf=", "scope="])
    except getopt.GetoptError:
        print('aws_sync_data.py --scope <scope> --conf <conf_file> --techconf <technical_conf_file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('aws_sync_data.py --scope <scope> --conf <conf_file> --techconf <technical_conf_file>')
            sys.exit()
        elif opt in ("-s", "--scope"):
            scope = arg
        elif opt in ("-c", "--conf"):
            conf_file = arg
        elif opt in ("-t", "--techconf"):
            tech_conf_file = arg
    print('- scope is: ' + scope)
    print('- configuration file is: ' + conf_file)
    print('- Technical Configuration file is: ' + tech_conf_file)
    return scope, conf_file, tech_conf_file


def add_month_to_current_date(delta_month):
    today = dt.date.today().replace(day=15)
    month = today + relativedelta(months=delta_month)
    return month.strftime("%Y%m")


def main(argv):
    scope, conf_file, tech_conf_file = get_args(argv)
    with open(conf_file) as file:
        conf = yaml.full_load(file)
    with open(tech_conf_file) as file:
        technical_conf = yaml.full_load(file)

    cds_bucket = technical_conf['env']['bucket_datalake_cds']
    ods_bucket = technical_conf['env']['bucket_datalake_ods']
    raw_bucket = technical_conf['env']['bucket_raw']

    def get_s3_sync_command(path):
        if path.startswith('ods/') or path.startswith('ods_supply/'):
            src_bucket = ods_bucket
        elif path.startswith('cds/') or path.startswith('cds_supply/'):
            src_bucket = cds_bucket
        else:
            raise Exception(f'The source: {path} does not belong to one of these directories (ods/, cds/, cds_supply/)')
        src = (src_bucket + '/' + path + '/').replace('//', '/')
        dst = (raw_bucket + '/datalake/' + path + '/').replace('//', '/')
        s3sync_command = f'aws s3 sync s3://{src} s3://{dst}' + \
                         ' --ignore-glacier-warnings --acl bucket-owner-full-control --delete'
        print('[aws_sync_data] launching command: ' + s3sync_command)
        return s3sync_command

    if scope not in conf['scope'].keys():
        raise Exception(f'{scope} is not a valid value of scope')

    scope_data = conf['scope'][scope]
    is_delta_scope = 'filter_time' in scope_data.keys()
    delta_months = scope_data['filter_time'] if 'filter_time' in scope_data.keys() else 0
    list_delta_tables = ['f_transaction_detail', 'f_delivery_detail', 'f_stock_picture']

    for source in scope_data['datalake']:
        for table in scope_data['datalake'][source]:
            if not is_delta_scope or table not in list_delta_tables:
                data_src = source + '/' + table
                os.system(f"{get_s3_sync_command(data_src)}")
            else:
                data_src = source + '/' + table
                for iter in range(delta_months):
                    month = add_month_to_current_date(-iter)
                    os.system(f"{get_s3_sync_command(data_src + '/' + month)}")


if __name__ == "__main__":
    main(sys.argv[1:])
