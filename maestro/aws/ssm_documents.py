"""
ssm_documents.py

AWS SSM Documents help methods - currently extending boto3

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
import os
import json
import boto3, botocore


def _log_and_print_to_console(msg, log_level='info'):
    """
    Print a message to the console and log it to a file
    :param msg: the message to print and log
    :param log_level: the logging level for the mesage
    """
    log_func = {'info': logging.info, 'warn': logging.warn, 'error': logging.error}
    print(msg)
    log_func[log_level.lower()](msg)


def parse_file_into_json_string(file_path):
    with open(file_path) as f:
        return json.dumps(json.load(f))


def get_document_from_region(requested_document, region, profile=None):
    if not region:
        _log_and_print_to_console("ERROR: You must supply a region", 'error')
        sys.exit(1)

    return_value = None

    session = boto3.session.Session(profile_name=profile, region_name=region)
    ssm = session.client('ssm')

    try:
        result = ssm.get_document(Name=requested_document)
        if result:
            if 'Content' in result:
                return_value = result['Content']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'InvalidDocument':
            return None
        else:
            _log_and_print_to_console('Unexpected error: %s' % e, 'error')

    return return_value


def get_document(region_list, requested_document, profile=None):
    result = {}

    for region in region_list:
        logging.debug("Checking region: " + region)
        result[region] = get_document_from_region(requested_document, region, profile)
    return result


def set_document_in_region(region, document_name, type, content, content_is_file=False, profile=None):
    if not region:
        _log_and_print_to_console("ERROR: You must supply a region", 'error')
        return False

    if not type:
        _log_and_print_to_console("ERROR: You must supply a type for the document - Command | Policy | Automation", 'error')
        return False

    if not content:
        _log_and_print_to_console("ERROR: You must supply content for the document", 'error')
        return False

    return_value = False

    session = boto3.session.Session(profile_name=profile, region_name=region)
    ssm = session.client('ssm')

    if content_is_file and not os.path.exists(content):
        _log_and_print_to_console("ERROR: File Value provided, but file does not exist", 'error')
        return False

    if content_is_file:
        content = parse_file_into_json_string(content)

    result = ssm.create_document(Name=document_name,
                               Content=content,
                               DocumentType=type)

    if result:
        if 'DocumentDescription' in result:
            # TODO: Should probably check the Status and react accordingly, but for now, as long as we have a
            #       DocumentDescription, assume all went well
            return_value = True
    return return_value


def set_document(region_list, document_name, type, content, content_is_file=False, profile=None):
    result = {}

    for region in region_list:
        logging.debug("Checking region: " + region)
        result[region] = set_document_in_region(region, document_name, type, content, content_is_file, profile)
    return result


def update_document_in_region(region, document_name, content, version=None, content_is_file=False, profile=None):
    if not region:
        _log_and_print_to_console("ERROR: You must supply a region", 'error')
        return False

    if not content:
        _log_and_print_to_console("ERROR: You must supply content for the document", 'error')
        return False

    return_value = False

    session = boto3.session.Session(profile_name=profile, region_name=region)
    ssm = session.client('ssm')

    if content_is_file and not os.path.exists(content):
        _log_and_print_to_console("ERROR: File Value provided, but file does not exist", 'error')
        return False

    if content_is_file:
        content = parse_file_into_json_string(content)

    if not version:
        version = '$LATEST'

    try:
        result = ssm.update_document(Name=document_name,
                                     Content=content,
                                     DocumentVersion=version)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'DuplicateDocumentContent':
            return True
        else:
            _log_and_print_to_console('Unexpected error: %s' % e, 'error')

    if result:
        if 'DocumentDescription' in result:
            # TODO: Should probably check the Status and react accordingly, but for now, as long as we have a
            #       DocumentDescription, assume all went well
            return_value = True
    return return_value


def update_document(region_list, document_name, content, version=None, content_is_file=False, profile=None):
    result = {}

    for region in region_list:
        logging.debug("Checking region: " + region)
        result[region] = update_document_in_region(region, document_name, content, version, content_is_file, profile)
    return result


def delete_document_in_region(region, document_name, profile=None):
    if not region:
        _log_and_print_to_console("ERROR: You must supply a region", 'error')
        return False

    if not document_name:
        _log_and_print_to_console("ERROR: You must supply a parameter to delete", 'error')
        return False

    return_value = False

    session = boto3.session.Session(profile_name=profile, region_name=region)
    ssm = session.client('ssm')

    try:
        result = ssm.delete_document(Name=document_name)
        if 'ResponseMetadata' in result:
            if 'HTTPStatusCode' in result['ResponseMetadata']:
                if result['ResponseMetadata']['HTTPStatusCode'] == 200:
                    return True
                else:
                    return False
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'InvalidDocument':
            _log_and_print_to_console('Document does not exist', 'warn')
            return False
        else:
            _log_and_print_to_console('Unexpected error: %s' % e)
            return False

    if result:
        if 'ResponseMetadata' in result:
            if 'HTTPStatusCode' in result['ResponseMetadata']:
                return_value = result['ResponseMetadata']['HTTPStatusCode']
    return return_value


def delete_document(region_list, document_name, profile=None):
    result = {}

    for region in region_list:
        logging.debug("Checking region: " + region)
        result[region] = delete_document_in_region(region, document_name, profile)
    return result



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='parameter_store.py')

    me_cmd_group = parser.add_mutually_exclusive_group(required=True)
    me_cmd_group.add_argument("--get", help="Perform a Get", action="store_true")
    me_cmd_group.add_argument("--set", help="Perform a Set", action="store_true")
    me_cmd_group.add_argument("--update", help="Perform an Update", action="store_true")
    me_cmd_group.add_argument("--delete", help="Perform a Delete", action="store_true")

    parser.add_argument("--name", help="Document name", dest='name', required=True)
    parser.add_argument("--type", help="Document type", dest='type')
    parser.add_argument("--version", help="Document version, if unspecified, '$LATEST' is used", dest='version')

    me_value_group = parser.add_mutually_exclusive_group()
    me_value_group.add_argument("--content", help="Document content. NOTE: should be a JSON string", dest='content')
    me_value_group.add_argument("--file-content", help="Use the contents of the given file for the content. NOTE: file contents should be valid JSON", dest='file_content')

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

    logging.basicConfig(filename='ssm_documents.log', format='%(asctime)s - %(levelname)7s : %(message)s',
                        level=log_level)

    logging.info("INIT")

    if args.get:
        result = get_document(args.regions, args.name, args.profile)
        for region in result:
            print(region + ': ' + ('\n' + result[region] if result[region] else "Not Present"))

    if args.set:
        if not args.type:
            _log_and_print_to_console("Must supply TYPE", "error")
            sys.exit(1)
        if not args.content and not args.file_content:
            _log_and_print_to_console("Must supply CONTENT", "error")
            sys.exit(1)
        content_is_file = True if args.file_content else False
        content = args.file_content if args.file_content else args.content
        result = set_document(args.regions, args.name, args.type, content, content_is_file, args.profile)
        for region in result:
            print(region + ': ' + ("Success" if result[region] else "Failed"))

    if args.update:
        if not args.content and not args.file_content:
            _log_and_print_to_console("Must supply CONTENT", "error")
            sys.exit(1)
        content_is_file = True if args.file_content else False
        content = args.file_content if args.file_content else args.content
        result = update_document(args.regions, args.name, content, args.version, content_is_file, args.profile)
        for region in result:
            print(region + ': ' + ("Success" if result[region] else "Failed"))

    if args.delete:
        result = delete_document(args.regions, args.name, args.profile)
        for region in result:
            print(region + ': ' + ("Success" if result[region] else "Failed"))
