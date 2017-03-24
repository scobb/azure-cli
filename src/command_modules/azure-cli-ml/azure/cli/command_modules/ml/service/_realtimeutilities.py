# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
Utilities to create and manage realtime web services.

"""

from __future__ import print_function

from datetime import datetime, timedelta
import os
import tarfile
import uuid

from azure.storage.blob import (BlockBlobService, ContentSettings, BlobPermissions)
from azuremlcli.cli_util import InvalidConfError
from azuremlcli.cli_util import is_int
import requests


class RealtimeConstants(object):
    supported_runtimes = ['spark-py', 'cntk-py', 'tensorflow-py', 'scikit-py']
    create_cmd_sample = "aml service create realtime -f <webservice file> -n <service name> [-m <model1> [-m <model2>] ...] [-p requirements.txt] [-s <schema>] [-r {0}]".format("|".join(supported_runtimes))  # pylint: disable=line-too-long


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


def try_add_sample_file(dependencies, schema_file, verbose):
    """
       Tries to find a sample file named after the given
       schema file. If found, adds it to the dependencies list.
    """

    if not os.path.exists(schema_file):
        if verbose:
            print("Error: no such path {}".format(schema_file))
        return False, ''
    else:
        sample_file = schema_file + '.sample'
        if not os.path.exists(sample_file):
            if verbose:
                print("No sample found named {}".format(sample_file))
            return False, ''
        if verbose:
            print("Adding {} to dependencies.".format(sample_file))
        dependencies.append(sample_file)

        # Return the basename of the file only, since dependencies
        # are always placed in the current directory inside the container
        return True, os.path.basename(sample_file.strip('/'))


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
            print("No valid ACS found. Please run 'aml env about' for instructions on setting up your environment.")
            print("")

    return marathon_base_url


def get_sample_data(sample_url, headers, verbose):
    """
    Try to retrieve sample data for the given service.
    :param sample_url: The url to the service
    :param headers: The headers to pass in the call
    :param verbose: Whether to print debugging info or not.
    :return: str - sample data if available, '' if not available, None if the service does not exist.
    """
    default_retval = None
    if verbose:
        print('[Debug] Fetching sample data from: {}'.format(sample_url))
    try:
        sample_data = requests.get(sample_url, headers=headers)
    except (requests.ConnectionError, requests.ConnectTimeout):
        if verbose:
            print('[Debug] Could not connect to sample data endpoint on this container.')
        return default_retval

    if sample_data.status_code == 404:
        if verbose:
            print('[Debug] Received a 404 - no sample route on this service.')
        return ''
    elif sample_data.status_code == 503:
        if verbose:
            print('[Debug] Received a 503 - no such service.')
        return default_retval
    elif sample_data.status_code != 200:
        if verbose:
            print('[Debug] Received {} - treating as no such service.'.format(sample_data.status_code))
        return default_retval

    try:
        sample_data = sample_data.json()
    except ValueError:
        if verbose:
            print('[Debug] Could not deserialize sample data. Malformed json {}.'.format(sample_data))
        return default_retval

    return str(sample_data)
