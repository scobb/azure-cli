# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
Utilities to create and manage realtime web services.

"""

from __future__ import print_function

import os
import tarfile
import uuid
import requests
import json

from .._util import InvalidConfError
from .._util import is_int


class RealtimeConstants(object):
    supported_runtimes = ['spark-py', 'cntk-py', 'tensorflow-py', 'scikit-py']
    ninja_runtimes = ['mrs']
    supported_logging_levels = ['none', 'info', 'debug', 'warn', 'trace']
    create_cmd_sample = "az ml service create realtime -f <webservice file> -n <service name> [-m <model1> [-m <model2>] ...] [-p requirements.txt] [-s <schema>] [-r {0}] [-l {1}]".format("|".join(supported_runtimes), "|".join(supported_logging_levels))  # pylint: disable=line-too-long

    swagger_uri_format = 'http://{0}/swagger.json'
    default_input_data = '!! YOUR DATA HERE !!'


def upload_dependency(context, dependency, verbose):
    """Uploads the named dependency as an asset to the provided azure storage account.
       If the dependency is a directory, it is zipped up before upload.

       Return values:
       -1,'': Error - path does not exist
       0, 'blob': Success, dependency uploaded to blob.
       1, 'blob': Success, dependency was a directory, uploaded to blob.
    """

    if not os.path.exists(dependency):
        if verbose:
            print('Error: no such path {}'.format(dependency))
        return -1, '', ''
    elif os.path.isfile(dependency):
        az_container_name = 'amlbdpackages'
        az_blob_name = os.path.basename(dependency)
        package_location = context.upload_dependency_to_azure_blob(dependency, az_container_name, az_blob_name)
        print(' {}'.format(dependency))
        return 0, package_location, az_blob_name
    elif os.path.isdir(dependency):
        arcname = os.path.basename(dependency.strip('/'))
        if verbose:
            print('[Debug] name in archive: {}'.format(arcname))
        az_blob_name = str(uuid.uuid4()) + '.tar.gz'
        tar_name = '/tmp/' + az_blob_name
        dependency_tar = tarfile.open(tar_name, 'w:gz')
        dependency_tar.add(dependency, arcname=arcname)
        dependency_tar.close()
        az_container_name = 'amlbdpackages'
        package_location = context.upload_dependency_to_azure_blob(tar_name, az_container_name, az_blob_name)
        print(' {}'.format(dependency))
        return 1, package_location, az_blob_name


def check_marathon_port_forwarding(context):
    """

    Check if port forwarding is set up to the ACS master
    :return: int - -1 if config error, 0 if direct cluster connection is set up, local port otherwise
    """
    try:
        conf = context.read_config()
        if not conf:
            return -1
    except InvalidConfError:
        return -1

    if 'port' in conf and is_int(conf['port']):
        return int(conf['port'])

    return -1


def resolve_marathon_base_url(context):
    """
    Determines the marathon endpoint of the configured ACS cluster
    :return: str - None if no marathon endpoint found, http://FQDN:[port] otherwise
    """
    marathon_base_url = None
    forwarded_port = check_marathon_port_forwarding(context)
    if forwarded_port > 0:
        marathon_base_url = 'http://127.0.0.1:' + str(forwarded_port)
    else:
        if os.environ.get('AML_ACS_MASTER') is not None:
            cluster = os.environ.get('AML_ACS_MASTER')
            marathon_base_url = 'http://' + cluster
        else:
            print("")
            print("No valid ACS found. Please run 'az ml env about' for instructions on setting up your environment.")
            print("")

    return marathon_base_url


def get_sample_data(sample_url, headers, verbose):
    """
    Try to retrieve sample data for the given service.
    :param sample_url: The url to the service's swagger definition
    :param headers: The headers to pass in the call
    :param verbose: Whether to print debugging info or not.
    :return: str - sample data if available, '' if not available, None if the service does not exist.
    """
    default_retval = None
    if verbose:
        print('[Debug] Fetching sample data from: {}'.format(sample_url))
    try:
        swagger_spec_response = requests.get(sample_url, headers=headers)
    except requests.ConnectionError:
        if verbose:
            print('[Debug] Could not connect to sample data endpoint on this container.')
        return default_retval

    if swagger_spec_response.status_code == 404:
        if verbose:
            print('[Debug] Received a 404 - no sample route on this service.')
        return ''
    elif swagger_spec_response.status_code == 503:
        if verbose:
            print('[Debug] Received a 503 - no such service.')
        return default_retval
    elif swagger_spec_response.status_code != 200:
        if verbose:
            print('[Debug] Received {} - treating as no such service.'.format(swagger_spec_response.status_code))
        return default_retval

    try:
        input_swagger = swagger_spec_response.json()['definitions']['ServiceInput']
        if 'example' in input_swagger:
            sample_data = input_swagger['example']
            return str(sample_data)
        else:
            return default_retval
    except ValueError:
        if verbose:
            print('[Debug] Could not deserialize swagger spec. Malformed json {}.'.format(swagger_spec_response))
        return default_retval


def get_service_swagger_spec(input_schema, output_schema):
    with open('data/service-swagger-template.json', 'r') as f:
        swagger_spec = json.load(f)
    if input_schema is not '':
        swagger_spec['definitions']['ServiceInput'] = _get_swagger_from_schema_file(input_schema, 'input')
    if output_schema is not '':
        swagger_spec['definitions']['ServiceOutput'] = _get_swagger_from_schema_file(output_schema, 'output')
    return swagger_spec


def _get_swagger_from_schema_file(schema_file_path, schema_type):
    if not (os.path.exists(schema_file_path) and os.path.isfile(schema_file_path)):
        raise ValueError("Invalid {0} schema file path: {1}. Value must point to an existing file.".format(
            schema_type, schema_file_path))
    with open(schema_file_path, 'r') as f:
        full_schema = json.load(f)
    if 'swagger' not in full_schema:
        raise ValueError("Invalid {0} schema content: missing 'swagger' element".format(schema_type))
    return full_schema['swagger']
