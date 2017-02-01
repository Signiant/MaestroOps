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

import sys
import boto3, botocore
import argparse
import logging


def _log_and_print_to_console(msg, log_level='info'):
    """
    Print a message to the console and log it to a file
    :param msg: the message to print and log
    :param log_level: the logging level for the mesage
    """
    log_func = {'info': logging.info, 'warn': logging.warn, 'error': logging.error}
    print(msg)
    log_func[log_level.lower()](msg)


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
        _log_and_print_to_console("ERROR: You must supply a region to scan", 'error')
        return None
    else:
        _log_and_print_to_console('Updating Stack: ' + stack_id)
        for param in parameters:
            if 'PreviousValue' in param:
                _log_and_print_to_console('   ' + param['ParameterKey'])
                _log_and_print_to_console('      OLD: ' + param['PreviousValue'])
                _log_and_print_to_console('      NEW: ' + param['ParameterValue'])
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
                    _log_and_print_to_console("   ERROR: New value matches Old value - no update required", 'error')
                else:
                    _log_and_print_to_console('Unexpected error: %s' % e)
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
        _log_and_print_to_console("ERROR: You must supply a region to scan", 'error')
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
        _log_and_print_to_console("ERROR: You must supply a region to scan", 'error')
        return None
    else:
        session = boto3.session.Session(profile_name=profile, region_name=region)
        cf_client = session.client('cloudformation')
        cf_data = cf_client.describe_stacks(StackName=stack_id)
        logging.debug("Getting stack description for stack with name/id: " + stack_id)
        if "Stacks" in cf_data:
            if len(cf_data["Stacks"]) > 1:
                # Problem - mutliple stacks with this name??
                _log_and_print_to_console("Error: Multiple stacks with given name", 'error')
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
                _log_and_print_to_console(
                    "Unexpected value detected - Stack will NOT be updated\n   Stack: " + stack[
                        'StackName'] + "\n   Existing value: " + parameter['ParameterValue'], 'warn')
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
        _log_and_print_to_console("ERROR: You must supply a region to scan", 'error')
    else:
        _log_and_print_to_console(
            "\nCloudFormation Stacks in region: " + region + " with at least one of the following parameters: " + ', '.join(
                parameter_list))
        stacks_with_given_parameter = get_stacks_with_given_parameter(region, parameter_list, profile=profile)
        if len(stacks_with_given_parameter) > 0:
            for stack in stacks_with_given_parameter:
                for parameter in stack["Parameters"]:
                    if parameter['ParameterKey'] in parameter_list:
                        _log_and_print_to_console(
                            "Stack Name: " + stack["StackName"] + "\n   " + parameter['ParameterKey'] + ": " +
                            parameter[
                                "ParameterValue"])
                        break
        else:
            _log_and_print_to_console("None")


def list_stacks_with_given_parameter(region_list, parameter_list, profile=None):
    """
    Print a list of stacks for the given regions that contain at least one parameter in the given parameter list
    :param region_list: list of regions
    :param parameter_list: list of possible parameter names
    """
    for region in region_list:
        list_stacks_with_given_parameter_in_region(region, parameter_list, profile=profile)


def update_stack_with_given_parameter(region, stack_name, expected_value, new_value, parameter_to_change, profile=None, dryrun=False,
                                      force=False):
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


def update_all_stacks_with_given_parameter(region, expected_value, new_value, parameter_to_change, profile=None, dryrun=False,
                                           force=False):
    """
    Update the given parameter in all stacks (for the given region) to a new value, provided
    the existing value contains the expected_value
    :param region: region where the stacks exist
    :param expected_value: the existing value of the parameter should contain the expected_value
    :param new_value: the new value for the parameter
    :param parameter_to_change: list of possible names for the parameter
    :param dryrun: if true, no changes are made
    """
    _log_and_print_to_console('\nUpdating all matching Stacks in region: ' + region)
    update_status = {}
    for stack in get_stacks_with_given_parameter(region, parameter_to_change, profile=profile):
        update_required, new_parameter_list = get_new_parameter_list_for_update(stack, expected_value, new_value,
                                                                                 parameter_to_change, force)
        if update_required:
            update_status[stack['StackId']] = _update_stack_parameters(region, stack["StackId"], new_parameter_list,
                                                                       profile=profile, dryrun=dryrun)

    return update_status


if __name__ == "__main__":

    logging.basicConfig(filename='cf_stack.log', format='%(asctime)s - %(levelname)7s : %(message)s',
                        level=logging.INFO)

    parser = argparse.ArgumentParser(
        description='Script to view/modify a Parameter in CloudFormation Stacks')

    parser.add_argument("--verbose", help="Turn on DEBUG logging", action='store_true', required=False)
    parser.add_argument("--param", help="space separated list of possible names for the parameter", dest='param',
                        nargs='+', required=True)
    parser.add_argument("--list",
                        help="List all stacks in a given region that have a given parameter",
                        dest='list', action='store_true', required=False)
    parser.add_argument("--regions", help="Specify regions (space separated)", dest='regions', required=True)
    parser.add_argument("--profile",
                        help="The name of an aws cli profile to use.", dest='profile', required=False)
    parser.add_argument("--update",
                        help="Update Parameter to new value for given stack in the specified region - must supply expected existing value and new value. STACK_ID can be the name or ID of the Stack",
                        dest='update', nargs=3, metavar=('STACK_ID', 'EXPECTED_VALUE', 'NEW_VALUE'), required=False)
    parser.add_argument("--update-all",
                        help="Update Parameter to new value for all stacks in the specified region - must supply expected existing value and new value",
                        dest='update_all', nargs=2, metavar=('EXPECTED_VALUE', 'NEW_VALUE'), required=False)
    parser.add_argument("--dryrun", help="Do a dryrun - no changes will be performed", dest='dryrun',
                        action='store_true', default=False, required=False)
    parser.add_argument("--force", help="Force an update, even if expected value doesn't match", dest='force',
                        action='store_true', default=False, required=False)
    args = parser.parse_args()

    log_level = logging.INFO

    if args.verbose:
        print("Verbose logging selected")
        log_level = logging.DEBUG

    logging.info("INIT")

    print('')

    if args.dryrun:
        _log_and_print_to_console("***** Dryrun selected - no changes will be made *****\n")

    _log_and_print_to_console("Searching for Stacks with a Parameter with name(s): " + ' '.join(args.param))

    if args.regions:
        _log_and_print_to_console("Regions: " + args.regions)

    if args.list:
        list_stacks_with_given_parameter(args.regions, args.param, args.profile)

    if args.update:
        if 'all' in args.regions:
            _log_and_print_to_console("Only one region can be specified with update", "error")
            sys.exit(1)
        update_status = update_stack_with_given_parameter(args.regions, args.update[0], args.update[1], args.update[2],
                                                          args.param, args.profile, args.dryrun, args.force)
        if not args.dryrun:
            if update_status:
                _log_and_print_to_console("\nStack Parameter Update Succeeded")
            else:
                _log_and_print_to_console("\nStack Parameter Update Failed")

    if args.update_all:
        if len(args.regions) > 1:
            _log_and_print_to_console("Only one region can be specified with update", "error")
            sys.exit(1)
        update_status = update_all_stacks_with_given_parameter(args.regions, args.update_all[0], args.update_all[1],
                                                               args.param, args.profile, args.dryrun, args.force)
        if not args.dryrun and update_status:
            _log_and_print_to_console("\n\nUPDATE STATUS:")
            for stack in update_status:
                _log_and_print_to_console(stack + ": " + ('Succeeded' if update_status[stack] else 'Failed'))

    _log_and_print_to_console('\nCOMPLETE')
