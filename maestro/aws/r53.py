"""
r53.py

Route 53 help methods - currently extending boto3

Note:
    Credentials are required to communicate with AWS.
    aws cli profile can be passed in using --profile, or
    the following ENVIRONMENT VARIABLES can be set before
    running this script:
        AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY
"""

import boto3, botocore
import argparse
import logging

logging.getLogger("botocore").setLevel(logging.CRITICAL)


def get_hosted_zone_list(profile=None):
    session = boto3.session.Session(profile_name=profile)
    r53_client = session.client('route53')
    result = None
    try:
        query = r53_client.list_hosted_zones()
        result = query['HostedZones']
    except botocore.exceptions.ClientError as e:
        logging.error('Unexpected error: %s' % e)
    return result


def get_all_records_in_zone(zone_id, profile=None):
    session = boto3.session.Session(profile_name=profile)
    r53_client = session.client('route53')
    resource_record_sets = []
    try:
        current_set = r53_client.list_resource_record_sets(HostedZoneId=zone_id)
        resource_record_sets.extend(current_set['ResourceRecordSets'])
        isTruncated = current_set['IsTruncated']
        while isTruncated:
            start_name = current_set['NextRecordName']
            current_set = r53_client.list_resource_record_sets(HostedZoneId=zone_id, StartRecordName=start_name)
            resource_record_sets.extend(current_set['ResourceRecordSets'])
            isTruncated = current_set['IsTruncated']
    except botocore.exceptions.ClientError as e:
        logging.error('Unexpected error: %s' % e)
    return resource_record_sets


def get_record_in_zone(zone_id, record_name, profile=None):
    session = boto3.session.Session(profile_name=profile)
    r53_client = session.client('route53')
    record_list = get_all_records_in_zone(zone_id, profile)
    wildcard = False
    if '*' in record_name:
        record_name = record_name.strip('*')
        wildcard = True
    else:
        if not record_name.endswith('.'):
            record_name += '.'
    result = []
    for record in record_list:
        if wildcard:
            if record_name in record['Name'].split('.')[0]:
                result.append(record)
        else:
            if record_name == record['Name']:
                result.append(record)
    return result


def get_hosted_zone_by_id(zone_id, profile=None):
    session = boto3.session.Session(profile_name=profile)
    r53_client = session.client('route53')
    result = None
    try:
        query = r53_client.get_hosted_zone(Id=zone_id)
        result = query['HostedZone']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchHostedZone':
            logging.error("Zone does not exist")
        else:
            logging.error('Unexpected error: %s' % e)
    return result


def get_hosted_zone_by_name(zone_name, profile=None):
    session = boto3.session.Session(profile_name=profile)
    r53_client = session.client('route53')
    result = None
    if not zone_name.endswith('.'):
        zone_name += '.'
    zone_list = get_hosted_zone_list(profile=profile)
    if zone_list:
        for zone in zone_list:
            if zone_name == zone['Name']:
                try:
                    query = r53_client.get_hosted_zone(Id=zone['Id'])
                    result = query['HostedZone']
                    break
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchHostedZone':
                        logging.error("Zone does not exist")
                    else:
                        logging.error('Unexpected error: %s' % e)
    return result


def zone_exists(zone_name, profile=None):
    zone_info = get_hosted_zone_by_name(zone_name, profile)
    if zone_info:
        return True
    else:
        return False


def record_as_fqdn(record_name, zone):
    record = record_name.split('.')
    if len(record) < 2:
        record_name = record_name + '.' + zone
    return record_name


def record_exists(record_name, zone_id=None, profile=None):
    alias = record_name
    if not zone_id:
        # Zone Not provided - figure it out
        record = record_name.split('.')
        if len(record) < 2:
            logging.error("Must supply FQDN if no zone provided")
            return False
        else:
            alias = record[0]
            zone = '.'.join(record[1:])
            zone_info = get_hosted_zone_by_name(zone, profile)
            if not zone_info:
                return False
            else:
                zone_id = zone_info['Id']
    else:
        if len(record_name.split('.')) > 1:
            # Record name to find is the first element
            alias = record_name.split('.')[0]
    records = get_all_records_in_zone(zone_id, profile)
    for record in records:
        if record['Name'].split('.')[0] == alias:
            # Found it
            return True
    return False


def _print_hosted_zone(zone_info):
    for key in zone_info:
        print("%23s: %s" % (key, zone_info[key]))
    print('')


def _print_resource_record(record):
    for key in record:
        print("%15s: %s" % (key, record[key]))
    print('')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Script to view/modify Route 53 entries')

    me_cmd_group = parser.add_mutually_exclusive_group(required=True)
    me_cmd_group.add_argument("--get", help="get Route 53 record(s)/zone(s) - wildcards supported", action='store_true')
    me_cmd_group.add_argument("--exists", help="determine if Route 53 record(s)/zone(s) exist", action='store_true')
    # TODO: --set, --delete, --update
    # me_cmd_group.add_argument("--set", help="set Route 53 record", action='store_true')
    # me_cmd_group.add_argument("--delete", help="delete Route 53 record", action='store_true')
    # me_cmd_group.add_argument("--update", help="update Route 53 record", action='store_true')

    parser.add_argument("--zones", help="The name of the hosted zone(s) (all is valid)",
                                 dest='zones', nargs='+', metavar='NAME')
    parser.add_argument("--records", help="The name of the resource record(s) (all is valid)",
                                 dest='records', nargs='+', metavar='NAME')

    parser.add_argument("--profile",
                        help="The name of an aws cli profile to use.", dest='profile', required=False)
    parser.add_argument("--verbose", help="Turn on DEBUG logging", action='store_true', required=False)
    args = parser.parse_args()

    log_level = logging.INFO

    if args.verbose:
        log_level = logging.DEBUG

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('r53.log')
    fh.setLevel(logging.DEBUG)
    # create console handler using level set in log_level
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    console_formatter = logging.Formatter('%(levelname)8s: %(message)s')
    ch.setFormatter(console_formatter)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)8s: %(message)s')
    fh.setFormatter(file_formatter)
    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    logging.debug("INIT")

    if not args.zones and not args.records:
        logging.error("Must supply either zones, records or both")

    if args.get:
        if not args.zones or 'all' in args.zones:
            # All zones requested - get the zone list
            zone_list = get_hosted_zone_list(profile=args.profile)
            for zone in zone_list:
                if args.records:
                    if 'all' in args.records:
                        # get all records for this zone
                        print("Getting all records for %s" % zone['Name'])
                        record_list = get_all_records_in_zone(zone['Id'], profile=args.profile)
                        for record in record_list:
                            _print_resource_record(record)
                    else:
                        # Get specific records in all zones
                        for record in args.records:
                            if not '*' in record:
                                record = record_as_fqdn(record, zone['Name'])
                            record_info = get_record_in_zone(zone['Id'], record, profile=args.profile)
                            for r in record_info:
                                _print_resource_record(r)
                else:
                    # No records requested - just print the zone
                    print(zone['Name'])
        else:
            # Specific zone(s) requested
            for zone in args.zones:
                zone_info = get_hosted_zone_by_name(zone, profile=args.profile)
                if zone_info:
                    if args.records:
                        if 'all' in args.records:
                            print("Getting all records for %s" % zone_info['Name'])
                            record_list = get_all_records_in_zone(zone_info['Id'], profile=args.profile)
                            for record in record_list:
                                _print_resource_record(record)
                        else:
                            # Get specific records in specifc zones
                            for record in args.records:
                                if not '*' in record:
                                    record = record_as_fqdn(record, zone_info['Name'])
                                record_info = get_record_in_zone(zone_info['Id'], record, profile=args.profile)
                                for r in record_info:
                                    _print_resource_record(r)
                    else:
                        # No records requested - just get zone info
                        _print_hosted_zone(zone_info)
                else:
                    logging.error("%s does not exist" % zone)
    elif args.exists:
        if (args.zones and 'all' in args.zones) or (args.records and 'all' in args.records):
            logging.error("all not supported with exists")

        if args.zones and args.records:
            # Looking for specific records in specific zones
            results = {}
            for zone in args.zones:
                if zone_exists(zone, profile=args.profile):
                    zone_info = get_hosted_zone_by_name(zone, profile=args.profile)
                    results[zone] = {}
                    for record in args.records:
                        results[zone][record] = record_exists(record, zone_info['Id'], profile=args.profile)
                else:
                    results[zone] = None
            for result in results:
                print("Zone: %s" % result)
                if results[result]:
                    for record in results[result]:
                        print ('   ' + record + ': ' + str(results[result][record]))
                else:
                    logging.error("   Zone doesn't exist")

        elif args.zones and not args.records:
            # Looking for specific zones
            results = {}
            for zone in args.zones:
                results[zone] = zone_exists(zone, profile=args.profile)
            for result in results:
                print(result + ': ' + str(results[result]))

        elif not args.zones and args.records:
            # Looking for specific records (without supplying zone info)
            results = {}
            for record in args.records:
                results[record] = record_exists(record, None, profile=args.profile)
            for result in results:
                print(result + ': ' + str(results[result]))
