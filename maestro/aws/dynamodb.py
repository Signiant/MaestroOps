"""
dynamodb.py

AWS Dynamo DB helper methods - currently extending boto3

Note:
    Credentials are required to communicate with AWS.
    aws cli profile can be passed in using --profile, or
    the following ENVIRONMENT VARIABLES can be set before
    running this script:
        AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY
"""

import logging
import argparse
import sys
import boto3, botocore


def get_all_tables_in_region(region, profile=None):
    if not region:
        logging.error("You must supply a region")
        return []

    session = boto3.session.Session(profile_name=profile, region_name=region)
    dynamodb = session.client('dynamodb')

    all_tables=[]
    try:
        current_set = dynamodb.list_tables()
        all_tables.extend(current_set['TableNames'])
        while 'LastEvaluatedTableName' in current_set:
            start_name = current_set['LastEvaluatedTableName']
            current_set = dynamodb.list_tables(ExclusiveStartTableName=start_name)
            all_tables.extend(current_set['TableNames'])
    except botocore.exceptions.ClientError as e:
        logging.error('Unexpected error: %s' % e)
        raise e

    return all_tables


def get_all_tables(region_list, profile=None):
    result = {}

    for region in region_list:
        logging.debug("Checking region: " + region)
        result[region] = get_all_tables_in_region(region, profile)
    return result


def table_exists_in_region(region, table_name, profile=None, suppress_warning=False):
    if not region:
        logging.error("You must supply a region")
        return []

    session = boto3.session.Session(profile_name=profile, region_name=region)
    dynamodb = session.client('dynamodb')

    result = False
    try:
        response = dynamodb.describe_table(TableName=table_name)
        if 'Table' in response:
            result = True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            if not suppress_warning:
                logging.warn("The given table %s does not exist in region %s" % (table_name, region))
        else:
            logging.error('Unexpected error: %s' % e)
            raise e

    return result


def table_exists(region_list, table_name, profile=None, suppress_warning=False):
    result = {}

    for region in region_list:
        logging.debug("Checking region: " + region)
        result[region] = table_exists_in_region(region, table_name, profile, suppress_warning)
    return result


def get_all_items_in_table_in_region(region, table_name, profile=None):
    if not region:
        logging.error("You must supply a region")
        return []

    all_items=[]
    # TODO: Implement this
    logging.error("Not yet implemented")

    return all_items


def get_all_items_in_table(region_list, table_name, profile=None):
    result = {}

    for region in region_list:
        logging.debug("Checking region: " + region)
        result[region] = get_all_items_in_table_in_region(region, table_name, profile)
    return result


def get_item_from_table_in_region(region, table_name, key, profile=None):
    if not region:
        logging.error("You must supply a region")
        return False

    session = boto3.session.Session(profile_name=profile, region_name=region)
    dynamodb = session.client('dynamodb')

    result = None

    try:
        response = dynamodb.get_item(TableName=table_name, Key=key)
        if 'ResponseMetadata' in response:
            if 'HTTPStatusCode' in response['ResponseMetadata']:
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    if 'Item' in response:
                        result = response['Item']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ValidationException':
            logging.error("The provided key element(s) do not match the schema")
        else:
            logging.error('Unexpected error: %s' % e)
        raise e

    return result


def get_item_from_table(region_list, table_name,
                        item_partition_key_value, item_partition_key_name, item_partition_key_type='S',
                        item_sort_key_value=None, item_sort_key_name=None, item_sort_key_type='S',
                        profile=None):
    result = {}

    # Build the key dict
    key={}
    key[item_partition_key_name] = {item_partition_key_type: item_partition_key_value}
    if item_sort_key_value:
        key[item_sort_key_name] = {item_sort_key_type: item_sort_key_value}


    for region in region_list:
        logging.debug("Checking region: " + region)
        if table_exists_in_region(region, table_name, profile):
            result[region] = get_item_from_table_in_region(region, table_name, key, profile)
        else:
            result[region] = None
    return result


def put_item_in_table_in_region(region, table_name, key, dryrun=False, profile=None):
    if not region:
        logging.error("You must supply a region")
        return False

    result = False
    # TODO: Implement this
    logging.error("Not yet implemented")

    return result


def put_item_in_table(region_list, table_name,
                      item_partition_key_value, item_partition_key_name, item_partition_key_type='S',
                      item_sort_key_value=None, item_sort_key_name=None, item_sort_key_type='S',
                      dryrun=False, profile=None):
    result = {}

    # Build the key dict
    key={}
    key[item_partition_key_name] = {item_partition_key_type: item_partition_key_value}
    if item_sort_key_value:
        key[item_sort_key_name] = {item_sort_key_type: item_sort_key_value}

    for region in region_list:
        logging.debug("Checking region: " + region)
        if table_exists_in_region(region, table_name, profile):
            result[region] = put_item_in_table_in_region(region, table_name, key, dryrun, profile)
        else:
            result[region] = False
    return result


def update_item_in_table_in_region(region, table_name, key, dryrun=False, profile=None):
    if not region:
        logging.error("You must supply a region")
        return False

    result = False
    # TODO: Implement this
    logging.error("Not yet implemented")

    return result


def update_item_in_table(region_list, table_name,
                         item_partition_key_value, item_partition_key_name, item_partition_key_type='S',
                         item_sort_key_value=None, item_sort_key_name=None, item_sort_key_type='S',
                         dryrun=False, profile=None):
    result = {}

    # Build the key dict
    key={}
    key[item_partition_key_name] = {item_partition_key_type: item_partition_key_value}
    if item_sort_key_value:
        key[item_sort_key_name] = {item_sort_key_type: item_sort_key_value}

    for region in region_list:
        logging.debug("Checking region: " + region)
        if table_exists_in_region(region, table_name, profile):
            result[region] = update_item_in_table_in_region(region, table_name, key, dryrun, profile)
        else:
            result[region] = False
    return result


def create_item_in_table_in_region(region, table_name, key, dryrun=False, profile=None):
    if not region:
        logging.error("You must supply a region")
        return False

    result = False
    # TODO: Implement this
    logging.error("Not yet implemented")

    return result


def create_item_in_table(region_list, table_name,
                         item_partition_key_value, item_partition_key_name, item_partition_key_type='S',
                         item_sort_key_value=None, item_sort_key_name=None, item_sort_key_type='S',
                         dryrun=False, profile=None):
    result = {}

    # Build the key dict
    key={}
    key[item_partition_key_name] = {item_partition_key_type: item_partition_key_value}
    if item_sort_key_value:
        key[item_sort_key_name] = {item_sort_key_type: item_sort_key_value}

    for region in region_list:
        logging.debug("Checking region: " + region)
        if table_exists_in_region(region, table_name, profile):
            result[region] = create_item_in_table_in_region(region, table_name, key, dryrun, profile)
        else:
            result[region] = False
    return result


def delete_item_in_table_in_region(region, table_name, key, dryrun=False, profile=None):
    if not region:
        logging.error("You must supply a region")
        return False

    result = False
    # TODO: Implement this
    logging.error("Not yet implemented")

    return result


def delete_item_in_table(region_list, table_name,
                         item_partition_key_value, item_partition_key_name, item_partition_key_type='S',
                         item_sort_key_value=None, item_sort_key_name=None, item_sort_key_type='S',
                         dryrun=False, profile=None):
    result = {}

    # Build the key dict
    key={}
    key[item_partition_key_name] = {item_partition_key_type: item_partition_key_value}
    if item_sort_key_value:
        key[item_sort_key_name] = {item_sort_key_type: item_sort_key_value}

    for region in region_list:
        logging.debug("Checking region: " + region)
        if table_exists_in_region(region, table_name, profile):
            result[region] = create_item_in_table_in_region(region, table_name, key, dryrun, profile)
        else:
            result[region] = False
    return result


def item_exists_in_region(region, table_name, key, profile=None):
    if get_item_from_table_in_region(region, table_name, key, profile):
        return True
    else:
        return False


def item_exists(region_list, table_name, item_partition_key_value, item_partition_key_name, item_partition_key_type='S',
                item_sort_key_value=None, item_sort_key_name=None, item_sort_key_type='S', profile=None):
    result = {}

    # Build the key dict
    key={}
    key[item_partition_key_name] = {item_partition_key_type: item_partition_key_value}
    if item_sort_key_value:
        key[item_sort_key_name] = {item_sort_key_type: item_sort_key_value}

    for region in region_list:
        logging.debug("Checking region: " + region)
        if table_exists_in_region(region, table_name, profile):
            result[region] = item_exists_in_region(region, table_name, key, profile)
        else:
            result[region] = False
    return result


def create_table_in_region(region, table_name, dryrun=False, profile=None):
    if not region:
        logging.error("You must supply a region")
        return False

    result = False
    # TODO: Implement this
    logging.error("Not yet implemented")

    return result


def create_table(region_list, table_name, dryrun=False, profile=None):
    result = {}
    for region in region_list:
        logging.debug("Checking region: " + region)
        if not table_exists_in_region(region, table_name, profile, suppress_warning=True):
            result[region] = create_table_in_region(region, table_name, dryrun, profile)
        else:
            logging.warn("Table %s already exists in region %s" % (table_name, region))
    return result


def delete_table_in_region(region, table_name, dryrun=False, profile=None):
    if not region:
        logging.error("You must supply a region")
        return False

    result = False
    # TODO: Implement this
    logging.error("Not yet implemented")

    return result


def delete_table(region_list, table_name, dryrun=False, profile=None):
    result = {}
    for region in region_list:
        logging.debug("Checking region: " + region)
        if table_exists_in_region(region, table_name, profile):
            result[region] = delete_table_in_region(region, table_name, dryrun, profile)
        else:
            result[region] = False
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='dynamodb.py')

    me_cmd_group = parser.add_mutually_exclusive_group(required=True)
    me_cmd_group.add_argument("--get", help="Perform a Get on an item", action="store_true")
    # me_cmd_group.add_argument("--put", help="Perform a Put on an item", action="store_true")
    # me_cmd_group.add_argument("--update", help="Perform an Update on an item", action="store_true")
    # me_cmd_group.add_argument("--list-all", help="List all items in the given table and regions", action="store_true", dest="list_all_items")
    # me_cmd_group.add_argument("--delete", help="Perform a Delete on an item/table", action="store_true")
    me_cmd_group.add_argument("--exists", help="Check if an item/table exists", action="store_true")
    # me_cmd_group.add_argument("--create", help="Create a table", action="store_true")
    me_cmd_group.add_argument("--list-all-tables", help="List all tables in the given regions", action="store_true", dest="list_all_tables")

    parser.add_argument("--table", help="Table name", dest='table', required=False)
    parser.add_argument("--item", help="Item name", dest='item', required=False)
    parser.add_argument("--pkey", help="Partition key", dest='pkey', required=False)
    parser.add_argument("--pkey-type", help="Partition key type", dest='pkey_type', default='S', required=False)
    parser.add_argument("--skey", help="Sort key", dest='skey', required=False)
    parser.add_argument("--skey-type", help="Sort key type", dest='skey_type', default='S', required=False)
    parser.add_argument("--skey-value", help="Sort key value", dest='skey_value', required=False)

    parser.add_argument("--regions", help="AWS Region(s) involved (space separated)", dest='regions', nargs='+', required=True)
    parser.add_argument("--profile",
                        help="The name of an aws cli profile to use.", dest='profile', required=False)
    parser.add_argument("--verbose", help="Turn on DEBUG logging", action='store_true', required=False)
    parser.add_argument("--dryrun", help="Do a dryrun - no changes will be performed", dest='dryrun',
                        action='store_true', default=False,
                        required=False)

    args = parser.parse_args()

    log_level = logging.INFO

    if args.verbose:
        print("Verbose logging selected")
        log_level = logging.DEBUG

    # Turn down logging for botocore
    logging.getLogger("botocore").setLevel(logging.CRITICAL)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('dynamodb.log')
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

    if not args.list_all_tables and not args.table:
        logger.error("Must supply a table")
        sys.exit(1)

    if args.item:
        if not args.pkey:
            logger.error("Must supply a partition key")
            sys.exit(1)
        if args.skey and not args.skey_value:
            logger.error("Sort key name present, but no sort key value provided")
            sys.exit(1)
        if args.skey_value and not args.skey:
            logging.warn("Sort key value present, but no sort key provided - will be ignored")

    # Item operations
    if args.get:
        if not args.item:
            logger.error("Must supply an item to get")
            sys.exit(1)

        result = get_item_from_table(args.regions, args.table,
                                     args.item, args.pkey, args.pkey_type,
                                     args.skey_value, args.skey, args.skey_type,
                                     args.profile)
        for region in result:
            print(region + ': ' + ('\n' + str(result[region]) if result[region] else "Not Present"))

    # if args.put:
    #     # TODO: Implement this above in put_item_in_region
    #     result = put_item_in_table(args.regions, args.table,
    #                                args.item, args.pkey, args.pkey_type,
    #                                args.skey_value, args.skey, args.skey_type,
    #                                args.dryrun, args.profile)
    #     for region in result:
    #         print(region + ': ' + ('\n' + 'Success' if result[region] else "Failed"))
    #
    # if args.update:
    #     # TODO: Implement this above in update_item_in_region
    #     result = update_item_in_table(args.regions, args.table,
    #                                   args.item, args.pkey, args.pkey_type,
    #                                   args.skey_value, args.skey, args.skey_type,
    #                                   args.dryrun, args.profile)
    #     for region in result:
    #         print(region + ': ' + ('\n' + 'Success' if result[region] else "Failed"))
    #
    # # Item/Table operations
    # if args.delete:
    #     if args.item:
    #         # Item delete
    #         # TODO: Implement this above in create_item_in_table_in_region
    #         result = delete_item_in_table(args.regions, args.table,
    #                                       args.item, args.pkey, args.pkey_type,
    #                                       args.skey_value, args.skey, args.skey_type,
    #                                       args.dryrun, args.profile)
    #     else:
    #         # Table delete
    #         # TODO: Implement this above in create_table_in_region
    #         result = delete_table(args.regions, args.table, args.dryrun, args.profile)
    #     for region in result:
    #         print(region + ': ' + ('\n' + 'Success' if result[region] else "Failed"))

    if args.exists:
        if args.item:
            # Item exists
            result = item_exists(args.regions, args.table,
                                 args.item, args.pkey, args.pkey_type,
                                 args.skey_value, args.skey, args.skey_type,
                                 args.profile)
        else:
            # Table exists
            if args.pkey or args.skey or args.skey_value:
                logging.warn("Ignoring extraneous information provided")
            result = table_exists(args.regions, args.table, args.profile, suppress_warning=True)
        for region in result:
            print(region + ': ' + str(result[region]))

    # Table operations
    # if args.list_all_items:
    #     # TODO: Implement this above in create_table_in_region
    #     result = get_all_items_in_table(args.regions, args.table, args.profile)
    #     for region in result:
    #         logger.info(region + ' - ' + str(len(result[region])) + ' table' + ('' if len(result[region]) == 1 else 's') + ' present')
    #         if len(result[region]) > 0:
    #             for table in result[region]:
    #                 logger.info('   ' + table)
    #
    # if args.create:
    #     if args.item:
    #         logger.warn("--item present with --create operation; Please use --put to create an item in a table")
    #     else:
    #         # Table create
    #         # TODO: Implement this above in create_table_in_region
    #         result = create_table(args.regions, args.table, args.dryrun, args.profile)
    #         for region in result:
    #             print(region + ': ' + ('\n' + 'Success' if result[region] else "Failed"))

    if args.list_all_tables:
        result = get_all_tables(args.regions, args.profile)
        for region in result:
            logger.info(region + ' - ' + str(len(result[region])) + ' table' + ('' if len(result[region]) == 1 else 's') + ' present')
            if len(result[region]) > 0:
                for table in result[region]:
                    logger.info('   ' + table)

