# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
batch_cli_util.py - Defines utilities, constants for batch portion of azureml CLI
"""

from __future__ import print_function

import os
from collections import OrderedDict

import requests

from .._util import ConditionalListTraversalFunction
from .._util import TraversalFunction
from .._util import ValueFunction
from .._util import get_json
from .._util import get_success_and_resp_str

# CONSTANTS
BATCH_URL_BASE_FMT = '{}'
BATCH_HEALTH_FMT = '{}/v1/health'
BATCH_DEPLOYMENT_INFO_FMT = '{}/v1/deploymentinfo'
BATCH_ALL_WS_FMT = '{}/v1/webservices'
BATCH_SINGLE_WS_FMT = '{}/{{{{}}}}'.format(BATCH_ALL_WS_FMT)
BATCH_ALL_JOBS_FMT = '{}/jobs'.format(BATCH_SINGLE_WS_FMT)
BATCH_SINGLE_JOB_FMT = '{}/{{{{}}}}'.format(BATCH_ALL_JOBS_FMT)
BATCH_CANCEL_JOB_FMT = '{}/cancel'.format(BATCH_SINGLE_JOB_FMT)
BATCH_PYTHON_ASSET = 'PythonAssets'
BATCH_JAR_ASSET = 'JarAssets'
BATCH_FILE_ASSET = 'FileAssets'

BATCH_EXTENSION_TO_ASSET_DICT = {'.py': BATCH_PYTHON_ASSET,
                                 '.jar': BATCH_JAR_ASSET}


# EXCEPTION CLASSES
class InvalidStorageException(Exception):
    """
    Exception raised when determining valid storage failsf
    """


# UTILITY FUNCTIONS
def batch_get_url(context, fmt, *args):
    """
    function to construct target url depending on whether in local mode or not
    :param context: CommandLineInterfaceContext object
    :param fmt: str format string to build url from
    :param args: list arguments to populate format string with
    :return:
    """
    base = 'http://localhost:8080' if context.in_local_mode() else \
        'https://{}-aml.apps.azurehdinsight.net'.format(context.hdi_domain)
    return fmt.format(base).format(*args)


def batch_get_asset_type(asset_id):
    """

    :param asset_id: str id of asset, expected form <name>.<extension>
    :return: str type of resource the asset's extension indicates
    """
    extension = os.path.splitext(asset_id)[1]
    if extension in BATCH_EXTENSION_TO_ASSET_DICT:
        return BATCH_EXTENSION_TO_ASSET_DICT[extension]

    return BATCH_FILE_ASSET


def batch_get_parameter_str(param_dict):
    """

    :param param_dict: dictionary of Parameter descriptions
    :return: formatted string for Usage associated with this parameter
    """
    letter = '--out' if param_dict['Direction'] == 'Output' else \
        ('--in' if param_dict['Kind'] == 'Reference' else '--param')
    ret_val = '{}={}:<value>'.format(letter, param_dict['Id'])
    return '[{}]'.format(ret_val) if 'Value' in param_dict else ret_val


def batch_get_job_description(context, http_content):
    """

    :param http_content: requests.content object with json encoded job
    :return: str value to print as job description
    """
    json_obj = get_json(http_content)
    return_str = 'Name: {}\n'.format(json_obj['WebServiceId'])
    return_str += 'JobId: {}\n'.format(json_obj['JobId'])
    if 'YarnAppId' in json_obj:
        return_str += 'YarnAppId: {}\n'.format(json_obj['YarnAppId'])
        return_str += 'Logs available at: https://{}.azurehdinsight.net/' \
                      'yarnui/hn/cluster/app/{}\n'.format(context.hdi_domain, json_obj['YarnAppId'])
    elif 'DriverLogFile' in json_obj:
        return_str += 'Logs available at: {}\n'.format(json_obj['DriverLogFile'])
    return_str += 'State: {}'.format(json_obj['State'])
    return return_str


def batch_create_parameter_entry(name, kind, direction):
    """

    :param name: str name of the parameter, in the form "<name>[=<default_value>]"
    :param kind: str kind of parameter (Reference|Value)
    :param direction: str direction of parameter (Input|Output)
    :return: dict encoding of the parameter for transmission to SparkBatch
    """
    return_value = {"Id": name,
                    "IsRuntime": True,
                    "IsOptional": False,
                    "Kind": kind,
                    "Direction": direction}
    if ':' in name:
        # need default value
        return_value['Id'] = name.split(':')[0]
        return_value['Value'] = ':'.join(name.split(':')[1:])

    return return_value


def batch_create_parameter_list(arg_list):
    """

    :param arg_list: list of tuples of the form [(name, direction, kind)]
            name: str name of the parameter, in the form "<name>[=<default_value>]"
            direction: str direction of the parameter (Input|Output)
            kind: str kind of the parameter (Reference|Value)
    :return: list of dicts encoding the parameters for transmission to SparkBatch
    """
    return [batch_create_parameter_entry(name, kind, direction)
            for (name, direction, kind) in arg_list]


def batch_app_is_installed(context):
    """

    :return: int response code, None if connection error
    """
    url = batch_get_url(context, BATCH_HEALTH_FMT)
    try:
        resp = context.http_call('get', url, auth=(context.hdi_user, context.hdi_pw))
        return resp.status_code
    except requests.exceptions.ConnectionError:
        return None


def batch_get_acceptable_storage(context):
    """

    :return: list of str - names of acceptable storage returned from the
    """
    url = batch_get_url(context, BATCH_DEPLOYMENT_INFO_FMT)
    try:
        success, content = get_success_and_resp_str(context, context.http_call('get', url,
                                                                               auth=(context.hdi_user, context.hdi_pw)))
    except requests.ConnectionError:
        raise InvalidStorageException(
            "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(
                url))

    if not success:
        raise InvalidStorageException(content)
    deployment_info = get_json(content)
    if 'Storage' not in deployment_info:
        raise InvalidStorageException('No storage found in deployment info.')

    return [info['Value'].strip() for info in deployment_info['Storage']]


def batch_env_is_valid(context):
    """

    :return: bool True if all of the following are true:
        1. environment specifies a SparkBatch location
        2. the app at that location is healthy
    """
    hdi_exists = False
    app_present = False
    if not context.in_local_mode() and (not context.hdi_domain or not context.hdi_user or not context.hdi_pw):
        print("")
        print("Environment is missing the following variables:")
        if not context.hdi_domain:
            print("  AML_HDI_CLUSTER")
        if not context.hdi_user:
            print("  AML_HDI_USER")
        if not context.hdi_pw:
            print("  AML_HDI_PW")
        print("For help setting up environment, run")
        print("  az ml env about")
        print("")
    else:
        hdi_exists = True

    # check if the app is installed via health api
    if hdi_exists:
        app_ping_return_code = batch_app_is_installed(context)
        if app_ping_return_code is None or app_ping_return_code == 404:
            print("AML Batch is not currently installed on {0}. "
                  "Please install the app.".format(batch_get_url(context,
                                                                 BATCH_URL_BASE_FMT,
                                                                 context.hdi_domain)))
        elif app_ping_return_code == 200:
            app_present = True
        elif app_ping_return_code == 403:
            print('Authentication failed on {}. Check your AML_HDI_USER and '
                  'AML_HDI_PW environment variables.'.format(
                      batch_get_url(context, BATCH_URL_BASE_FMT, context.hdi_domain)))
            print("For help setting up environment, run")
            print("  az ml env about")
            print("")
        else:
            print('Unexpected return code {} when querying AzureBatch '
                  'at {}.'.format(app_ping_return_code,
                                  batch_get_url(context, BATCH_URL_BASE_FMT, context.hdi_domain)))
            print("If this error persists, contact the SparkBatch team for more "
                  "information.")
    return hdi_exists and app_present


def batch_env_and_storage_are_valid(context):
    """

    :return: bool True if all of the following are true:
        1. environment specifies a SparkBatch location
        2. the app at that location is healthy
        3. storage is defined in the environment
        4. the storage matches the storage associated with the SparkBatch app (for HDI)
    """
    if not batch_env_is_valid(context):
        return False

    if context.in_local_mode():
        return True

    if not context.az_account_name or not context.az_account_key:
        print("")
        print("Environment is missing the following variables:")
        if not context.az_account_name:
            print("  AML_STORAGE_ACCT_NAME")
        if not context.az_account_key:
            print("  AML_STORAGE_ACCT_KEY.")
        print("For help setting up environment, run")
        print("  az ml env about")
        print("")
        return False

    try:
        acceptable_storage = batch_get_acceptable_storage(context)
    except InvalidStorageException as exc:
        print("Error retrieving acceptable storage from SparkBatch: {}".format(exc))
        return False

    if context.az_account_name not in acceptable_storage:
        print("Environment storage account {0} not found when querying server "
              "for acceptable storage. Available accounts are: "
              "{1}".format(context.az_account_name, ', '.join(acceptable_storage)))
        return False

    return True


def batch_get_job(context, job_name, service_name, verbose=False):
    """

    :param context: CommandLineInterfaceContext object
    :param job_name: str name of job to get
    :param service_name: str name of service that job belongs to
    :param verbose: bool verbosity flag
    :return:
    """
    url = batch_get_url(context, BATCH_SINGLE_JOB_FMT, service_name, job_name)
    if verbose:
        print("Getting resource at {}".format(url))
    try:
        return context.http_call('get', url, auth=(context.hdi_user, context.hdi_pw))
    except requests.ConnectionError:
        print("Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return


class BatchEnvironmentFunction(ValueFunction):
    """
    ValueFunction object for use displaying the current environment
    """
    def evaluate(self, context):
        return batch_get_url(context, BATCH_URL_BASE_FMT)


class ScoringUrlFunction(ValueFunction):
    """
    ValueFunction object for use displaying API endpoint of a service
    """
    def evaluate(self, context):
        return batch_get_url(context,
                             BATCH_SINGLE_JOB_FMT, self.json_obj['Id'],
                             '<job_id>')

batch_create_service_header_to_fn_dict = OrderedDict(
    [('Name', TraversalFunction(('Id',))), ('Environment', BatchEnvironmentFunction())])


batch_list_service_header_to_fn_dict = OrderedDict(
    [('Name', TraversalFunction(('Name',))),
     ('Last_Modified_at', TraversalFunction(('ModificationTimeUtc',))),
     ('Environment', BatchEnvironmentFunction())
    ])

batch_list_jobs_header_to_fn_dict = OrderedDict(
    [('Name', TraversalFunction(('Name',))),
     ('Last_Modified_at', TraversalFunction(('ModificationTimeUtc',))),
     ('Environment', BatchEnvironmentFunction())])

batch_view_service_header_to_fn_dict = OrderedDict(
    [('Name', TraversalFunction(('Id',))),
     ('Environment', BatchEnvironmentFunction())])

batch_view_service_usage_header_to_fn_dict = OrderedDict(
    [('Scoring_url', ScoringUrlFunction()),
     ('Inputs',
      ConditionalListTraversalFunction(
          ('Parameters',),
          condition=lambda param: (param['Kind'] == 'Reference' and
                                   param['Direction'] == 'Input'),
          action=lambda param: param['Id'])),
     ('Outputs', ConditionalListTraversalFunction(
         ('Parameters',),
         condition=lambda param: (param['Kind'] == 'Reference' and
                                  param['Direction'] == 'Output'),
         action=lambda param: param['Id'])),
     ('Parameters', ConditionalListTraversalFunction(
         ('Parameters',),
         condition=lambda param: (param['Kind'] == 'Value' and
                                  param['Direction'] == 'Input'),
         action=lambda param: param['Id']))
    ])


def validate_and_split_run_param(raw_param):
    """

    :param raw_param: str parameter in form <key>:<value>
    :return: (bool, str, str) - (valid, key, value)
    """
    if ':' not in raw_param:
        print("Must provide value for service parameter {0}".format(raw_param))
        return False, None, None
    else:
        return True, raw_param.split(':')[0], ':'.join(raw_param.split(':')[1:])
