"""
cf_stack.py

CloudFormation Stack help methods - currently extending boto3

Note:
    Credentials are required to communicate with AWS.
    aws cli profile can be passed in using --profile, or
    the following ENVIRONMENT VARIABLES can be set before
    running this script:
        AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY
"""

import sys, os
import boto3, botocore
import argparse
import json, yaml
import logging


def read_from_file(file_path):
    '''
    Read from a file and return a dict
    :param file_path: path to file
    :return: dict
    '''
    template_body = {}
    if os.path.exists(file_path):
        with open(file_path, 'r') as stream:
            if file_path.endswith('.yaml'):
                template_body = yaml.load(stream, yaml.SafeLoader)
            else:
                # Assume json if not yaml
                template_body = json.load(stream)
    else:
        logging.error('given file: ' + file_path + ' does not exist')
    return template_body


def process_stack_params_arg(stack_params):
    '''
    stack_params should be a list of key=value pairs
    :param stack_params: list of 'key=value' strings
    :return: list of dicts
    '''
    stack_parameters=[]
    for param in stack_params:
        key,value = param.split('=')
        stack_parameters.append({'ParameterKey': key, 'ParameterValue': value})
    return stack_parameters


def update_stack_in_region(region, stack_name, stack_params, template_body, new_stack=False, profile=None, dryrun=False):
    '''
    Update a stack in the given region
    :param region: region to create/update the stack in
    :param stack_name: name of the stack
    :param stack_params: stack parameters (list of dicts)
    :param template_body: body of the template as a dict
    :return: ARN of the stack, if created or None
    '''
    session = boto3.session.Session(profile_name=profile, region_name=region)
    cf_client = session.client('cloudformation')

    result = None

    create = False
    if new_stack:
        create = True

    # TODO: create_change_set

    # Check if the stack exists
    try:
        cf_client.describe_stacks(StackName=stack_name)
        if new_stack:
            logging.error('Cannot create - stack already exists - use update to update it')
            return result
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ValidationError':
            # Stack doesn't exist - set create to True
            create = True
        else:
            logging.error('Unexpected error: %s' % e)

    # Validate the template
    logging.debug("\nTemplate body:\n" + str(template_body))
    try:
        cf_client.validate_template(TemplateBody=json.dumps(template_body))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ValidationError':
            logging.error('Validation of given template failed')
        else:
            logging.error('Unexpected error: %s' % e)
        return result

    logging.debug("\nStack params:\n" + str(stack_params))


    if create:
        response = cf_client.create_stack(StackName=stack_name,
                                          TemplateBody=json.dumps(template_body),
                                          Parameters=stack_params)
    else:
        response = cf_client.update_stack(StackName=stack_name,
                                          TemplateBody=json.dumps(template_body),
                                          Parameters=stack_params)

    if 'ResponseMetadata' in response and 'HTTPStatusCode' in response['ResponseMetadata'] \
            and response['ResponseMetadata']['HTTPStatusCode'] == 200:
        if 'StackId' in response:
            result = response['StackId']
    return result


def update_stack(region_list, stack_name, stack_params, template_body, profile=None, dryrun=False):
    '''
    Create/Update a stack in the given regions
    :param region_list: list of regions to create the stack in
    :param stack_name: name of the stack
    :param stack_params: stack parameters (list of dicts)
    :param template_body: body of the template as a dict
    :return: ARN of the stack(s) or None if not created
    '''
    result = {}

    for region in region_list:
        logging.debug("Creating stack in region: " + region)
        result[region] = update_stack_in_region(region, stack_name, stack_params, template_body,
                                                profile=profile, dryrun=dryrun)
    return result


def create_stack(region_list, stack_name, stack_params, template_body, profile=None, dryrun=False):
    '''
    Create a stack in the given regions
    :param region_list: list of regions to create the stack in
    :param stack_name: name of the stack
    :param stack_params: stack parameters (list of dicts)
    :param template_body: body of the template as a dict
    :return: ARN of the stack(s) or None if not created
    '''
    result = {}

    for region in region_list:
        logging.debug("Creating stack in region: " + region)
        result[region] = update_stack_in_region(region, stack_name, stack_params, template_body, new_stack=True,
                                                profile=profile, dryrun=dryrun)
    return result


def _update_stack_parameters(region, stack_id, parameters, profile=None, dryrun=False):
    """
    Update a given stack with the given parameters
    :param region: region that the stack exists in
    :param stack_id: the name or ID of the stack
    :param parameters: list of parameter objects
    :param dryrun: if true, no changes are made
    :return: json object
    """
    if not region:
        logging.error("ERROR: You must supply a region to scan")
        return None
    else:
        logging.info('Updating Stack: ' + stack_id)
        for param in parameters:
            if 'PreviousValue' in param:
                logging.info('   ' + param['ParameterKey'])
                logging.info('      OLD: ' + param['PreviousValue'])
                logging.info('      NEW: ' + param['ParameterValue'])
                del param['PreviousValue']
        if not dryrun:
            stack = get_stack_with_name_or_id(region, stack_id)
            session = boto3.session.Session(profile_name=profile, region_name=region)
            cf_client = session.client('cloudformation')
            try:
                if 'Capabilities' in stack:
                    status = cf_client.update_stack(StackName=stack_id, Parameters=parameters, UsePreviousTemplate=True, Capabilities=stack['Capabilities'])['StackId']
                else:
                    status = cf_client.update_stack(StackName=stack_id, Parameters=parameters, UsePreviousTemplate=True)['StackId']
                return status
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'ValidationError' and 'No updates are to be performed' in \
                        e.response['Error']['Message']:
                    logging.error("   ERROR: New value matches Old value - no update required")
                else:
                    logging.error('Unexpected error: %s' % e)
    return False


def get_stacks_with_given_parameter(region, parameter_list, profile=None):
    """
    Get a list of stacks that have at least one parameter with a name in the given list
    :param region: the region to scan
    :param parameter_list: list of possible parameter names to look for
    :return: list of stacks that have a parameter with a name in the given list
    """
    stacks_with_given_parameter = []
    if not region:
        logging.error("ERROR: You must supply a region to scan")
        return None
    else:
        session = boto3.session.Session(profile_name=profile, region_name=region)
        cf_client = session.client('cloudformation')
        cf_data = cf_client.describe_stacks()
        if "Stacks" in cf_data:
            for stack in cf_data["Stacks"]:
                if "Parameters" in stack:
                    for parameter in stack["Parameters"]:
                        if parameter['ParameterKey'] in parameter_list:
                            logging.debug(
                                "Found parameter - " + parameter['ParameterKey'] + ' - in stack: ' + stack['StackName'])
                            stacks_with_given_parameter.append(stack)
                            break
    return stacks_with_given_parameter


def get_stack_with_name_or_id(region, stack_id, profile=None):
    """
    Get the stack with the given name or ID
    :param region: region where the stack exists
    :param stack_id: stack name or ID
    :return: stack in question or empty dictionary
    """
    stack = {}
    if not region:
        logging.error("ERROR: You must supply a region to scan")
        return None
    else:
        session = boto3.session.Session(profile_name=profile, region_name=region)
        cf_client = session.client('cloudformation')
        cf_data = cf_client.describe_stacks(StackName=stack_id)
        logging.debug("Getting stack description for stack with name/id: " + stack_id)
        if "Stacks" in cf_data:
            if len(cf_data["Stacks"]) > 1:
                # Problem - mutliple stacks with this name??
                logging.error("Error: Multiple stacks with given name")
            else:
                stack = cf_data['Stacks'][0]
    return stack


def get_new_parameter_list_for_update(stack, expected_value, new_value, parameter_to_change, force=False):
    """
    Given a list of possible parameter names, generate a new parameter list with
    existing_value changed to new_value
    :param stack: the stack to mine the parameter list from
    :param expected_value: expected existing value for the parameter
    :param new_value: the new value to use for the parameter
    :param parameter_to_change: list containing possible parameter names
    :return: tuple containing a boolean for whether an update is required and the new parameter list
    """
    new_parameters_list = []
    update_required = False
    for parameter in stack['Parameters']:
        new_param = {}
        new_param['ParameterKey'] = parameter['ParameterKey']
        if parameter['ParameterKey'] in parameter_to_change:
            if force or (expected_value in parameter['ParameterValue']):
                update_required = True
                new_param['ParameterValue'] = new_value
                new_param['PreviousValue'] = parameter['ParameterValue']
            else:
                logging.warn(
                    "Unexpected value detected - Stack will NOT be updated\n   Stack: " + stack[
                        'StackName'] + "\n   Existing value: " + parameter['ParameterValue'])
                new_param['UsePreviousValue'] = True
        else:
            new_param['UsePreviousValue'] = True
        new_parameters_list.append(new_param)
    return update_required, new_parameters_list


def list_stacks_with_given_parameter_in_region(region, parameter_list, profile=None):
    """
    Print a list of stacks in a given region that contain at least one parameter in the given parameter list
    :param region: region to scan
    :param parameter_list: list of possible parameter names
    """
    if not region:
        logging.error("You must supply a region to scan")
    else:
        logging.info(
            "\nCloudFormation Stacks in region: " + region + " with at least one of the following parameters: " + ', '.join(
                parameter_list))
        stacks_with_given_parameter = get_stacks_with_given_parameter(region, parameter_list, profile=profile)
        if len(stacks_with_given_parameter) > 0:
            for stack in stacks_with_given_parameter:
                for parameter in stack["Parameters"]:
                    if parameter['ParameterKey'] in parameter_list:
                        logging.info(
                            "Stack Name: " + stack["StackName"] + "\n   " + parameter['ParameterKey'] + ": " +
                            parameter[
                                "ParameterValue"])
                        break
        else:
            logging.info("None")


def list_stacks_with_given_parameter(region_list, parameter_list, profile=None):
    """
    Print a list of stacks for the given regions that contain at least one parameter in the given parameter list
    :param region_list: list of regions
    :param parameter_list: list of possible parameter names
    """
    for region in region_list:
        list_stacks_with_given_parameter_in_region(region, parameter_list, profile=profile)


def update_stack_with_given_parameter(region, stack_name, expected_value, new_value, parameter_to_change, profile=None,
                                      dryrun=False, force=False):
    """
    Update the given parameter in the given stack (for the given region) to a new value, provided
    the existing value contains the expected_value
    :param region: region where the stack exists
    :param stack_name: the stack name or ID
    :param expected_value: the existing value of the parameter should contain the expected_value
    :param new_value: the new value for the parameter
    :param parameter_to_change: list of possible names for the parameter
    :param dryrun: if true, no changes are made
    :returns: True / False
    """
    stack = get_stack_with_name_or_id(region, stack_name)
    update_required, new_parameter_list = get_new_parameter_list_for_update(stack, expected_value, new_value,
                                                                             parameter_to_change, force)
    if update_required:
        return _update_stack_parameters(region, stack["StackId"], new_parameter_list, profile=profile, dryrun=dryrun)


def update_all_stacks_with_given_parameter(region, expected_value, new_value, parameter_to_change, profile=None,
                                           dryrun=False, force=False):
    """
    Update the given parameter in all stacks (for the given region) to a new value, provided
    the existing value contains the expected_value
    :param region: region where the stacks exist
    :param expected_value: the existing value of the parameter should contain the expected_value
    :param new_value: the new value for the parameter
    :param parameter_to_change: list of possible names for the parameter
    :param dryrun: if true, no changes are made
    """
    logging.info('\nUpdating all matching Stacks in region: ' + region)
    update_status = {}
    for stack in get_stacks_with_given_parameter(region, parameter_to_change, profile=profile):
        update_required, new_parameter_list = get_new_parameter_list_for_update(stack, expected_value, new_value,
                                                                                 parameter_to_change, force)
        if update_required:
            update_status[stack['StackId']] = _update_stack_parameters(region, stack["StackId"], new_parameter_list,
                                                                       profile=profile, dryrun=dryrun)

    return update_status


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Script to view/modify CloudFormation Stacks')

    parser.add_argument("--create-stack", help="create stack in given region(s)", dest='create_stack', required=False)
    parser.add_argument("--update-stack", help="update stack in given region(s)", dest='update_stack', required=False)
    parser.add_argument("--stack-params", help="space separated list of key=value stack parameters", dest='stack_params',
                        nargs='+', required=False)
    parser.add_argument("--template-body", help="CFN template body (as json/yaml - preface with file:// if file)", dest='template_body',
                        required=False)
    parser.add_argument("--list",
                        help="List all stacks in given region(s) that have a given parameter",
                        dest='list', action='store_true', required=False)
    parser.add_argument("--update",
                        help="Update Parameter to new value for given stack in the specified region - must supply expected existing value and new value. STACK_ID can be the name or ID of the Stack",
                        dest='update', nargs=3, metavar=('STACK_ID', 'EXPECTED_VALUE', 'NEW_VALUE'), required=False)
    parser.add_argument("--param", help="space separated list of possible names for the parameter", dest='param',
                        nargs='+', required=False)
    parser.add_argument("--update-all",
                        help="Update Parameter to new value for all stacks in the specified region - must supply expected existing value and new value",
                        dest='update_all', nargs=2, metavar=('EXPECTED_VALUE', 'NEW_VALUE'), required=False)
    parser.add_argument("--regions", help="Specify regions (space separated)", dest='regions', nargs='+', required=True)
    parser.add_argument("--profile",
                        help="The name of an aws cli profile to use.", dest='profile', required=False)
    parser.add_argument("--dryrun", help="Do a dryrun - no changes will be performed", dest='dryrun',
                        action='store_true', default=False, required=False)
    parser.add_argument("--force", help="Force an update, even if expected value doesn't match", dest='force',
                        action='store_true', default=False, required=False)
    parser.add_argument("--verbose", help="Turn on DEBUG logging", action='store_true', required=False)
    args = parser.parse_args()

    log_level = logging.INFO

    if args.verbose:
        log_level = logging.DEBUG

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('cf_stack.log')
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

    if args.dryrun:
        logging.info("***** Dryrun selected - no changes will be made *****\n")

    if args.regions:
        logging.info("Regions: " + str(args.regions))

    if not args.param:
        if args.list or args.update or args.update_all:
            logging.critical("Must supply a parameter")
            sys.exit(1)

    if args.create_stack or args.update_stack:
        if not args.template_body:
            logging.critical("Must supply a template body")
            sys.exit(1)

        if args.template_body.startswith('file://'):
            # Template is a file - read it in
            file_path = args.template_body[7:]
            template_body = read_from_file(file_path)
            if not template_body:
                sys.exit(1)
        else:
            template_body = args.template_body

        stack_params = process_stack_params_arg(args.stack_params)

        result = None
        if args.create_stack:
            result = create_stack(args.regions, args.create_stack, stack_params, template_body, args.profile, args.dryrun)
        elif args.update_stack:
            result = update_stack(args.regions, args.update_stack, stack_params, template_body, args.profile, args.dryrun)

        if not args.dryrun:
            if result:
                logging.info("Create/Update results:\n")
                for region in result:
                    logging.info(region + ': ' + ("Stack ARN: " + result[region] if result[region] else "Failed"))
            else:
                logging.error("Create/Update failed.\n")

    elif args.list:
        if not args.param:
            logging.critical("Must supply a parameter name")
            sys.exit(1)
        logging.info("Searching for Stacks with a Parameter with name(s): " + ' '.join(args.param))
        list_stacks_with_given_parameter(args.regions, args.param, args.profile)

    elif args.update:
        if 'all' in args.regions:
            logging.critical("Only one region can be specified with update", "error")
            sys.exit(1)
        if not args.param:
            logging.critical("Must supply a parameter name")
            sys.exit(1)
        update_status = update_stack_with_given_parameter(args.regions, args.update[0], args.update[1], args.update[2],
                                                          args.param, args.profile, args.dryrun, args.force)
        if not args.dryrun:
            if update_status:
                logging.info("\nStack Parameter Update Succeeded")
            else:
                logging.error("\nStack Parameter Update Failed")

    elif args.update_all:
        if len(args.regions) > 1:
            logging.critical("Only one region can be specified with update", "error")
            sys.exit(1)
        if not args.param:
            logging.critical("Must supply a parameter name")
            sys.exit(1)
        update_status = update_all_stacks_with_given_parameter(args.regions, args.update_all[0], args.update_all[1],
                                                               args.param, args.profile, args.dryrun, args.force)
        if not args.dryrun and update_status:
            logging.info("\n\nUPDATE STATUS:")
            for stack in update_status:
                logging.info(stack + ": " + ('Succeeded' if update_status[stack] else 'Failed'))
