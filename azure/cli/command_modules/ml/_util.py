# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
Utility functions for AML CLI
"""

from __future__ import print_function
import os
import json
import sys
import platform
import socket
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend

try:
    # python 3
    from urllib.request import pathname2url
    from urllib.parse import urljoin, urlparse  # pylint: disable=unused-import
except ImportError:
    # python 2
    from urllib import pathname2url
    from urlparse import urljoin, urlparse

import subprocess
import re
import shutil
import requests
from tabulate import tabulate
from builtins import input
from azure.storage.blob import (BlobPermissions, BlockBlobService, ContentSettings)

ice_base_url = 'https://amlacsagent.azureml-int.net'
acs_connection_timeout = 5
ice_connection_timeout = 5


# EXCEPTIONS
class InvalidConfError(Exception):
    """Exception raised when config read from file is not valid json."""
    pass


# CONTEXT CLASS
class CommandLineInterfaceContext(object):
    """
    Context object that handles interaction with shell, filesystem, and azure blobs
    """
    hdi_home_regex = r'(.*:\/\/)?(?P<cluster_name>[^\s]*)'
    aml_env_default_location = 'east us'
    az_account_name = os.environ.get('AML_STORAGE_ACCT_NAME')
    az_account_key = os.environ.get('AML_STORAGE_ACCT_KEY')
    app_insights_account_name = os.environ.get('AML_APP_INSIGHTS_NAME')
    app_insights_account_key = os.environ.get('AML_APP_INSIGHTS_KEY', '')
    acs_master_url = os.environ.get('AML_ACS_MASTER')
    acs_agent_url = os.environ.get('AML_ACS_AGENT')
    acr_home = os.environ.get('AML_ACR_HOME')
    acr_user = os.environ.get('AML_ACR_USER')
    acr_pw = os.environ.get('AML_ACR_PW')
    hdi_home = os.environ.get('AML_HDI_CLUSTER')
    hdi_user = os.environ.get('AML_HDI_USER', '')
    hdi_pw = os.environ.get('AML_HDI_PW', '')
    env_is_k8s = os.environ.get('AML_ACS_IS_K8S', '')

    def __init__(self):
        self.config_path = os.path.join(get_home_dir(), '.amlconf')
        self.az_container_name = 'azureml'
        if self.hdi_home:
            outer_match_obj = re.match(self.hdi_home_regex, self.hdi_home)
            if outer_match_obj:
                self.hdi_home = outer_match_obj.group('cluster_name')
        self.hdi_domain = self.hdi_home.split('.')[0] if self.hdi_home else None

    @staticmethod
    def str_from_subprocess_communicate(output):
        """

        :param output: bytes or str object
        :return: str version of output
        """
        if isinstance(output, bytes):
            return output.decode('utf-8')
        return output

    def run_cmd(self, cmd):
        """

        :param cmd: str command to run
        :return: str, str - std_out, std_err
        """
        proc = subprocess.Popen(cmd, shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        output, err = proc.communicate()
        return self.str_from_subprocess_communicate(output), \
               self.str_from_subprocess_communicate(err)

    def read_config(self):
        """

        Tries to read in ~/.amlconf as a dictionary.
        :return: dict - if successful, the config dictionary from ~/.amlconf, None otherwise
        :raises: InvalidConfError if the configuration read is not valid json, or is not a dictionary
        """
        try:
            with open(self.config_path) as conf_file:
                conf = conf_file.read()
        except IOError:
            return None
        try:
            conf = json.loads(conf)
        except ValueError:
            raise InvalidConfError

        if not isinstance(conf, dict):
            raise InvalidConfError

        return conf

    def write_config(self, conf):
        """

        Writes out the given configuration dictionary to ~/.amlconf.
        :param conf: Configuration dictionary.
        :return: 0 if success, -1 otherwise
        """
        try:
            with open(self.config_path, 'w') as conf_file:
                conf_file.write(json.dumps(conf))
        except IOError:
            return -1

        return 0

    def in_local_mode(self):
        """
        Determines if AML CLI is running in local mode
        :return: bool - True if in local mode, false otherwise
        """

        try:
            conf = self.read_config()
            if conf and 'mode' in conf:
                return conf['mode'] == 'local'
        except InvalidConfError:
            print('Warning: Azure ML configuration file is corrupt.')
            print('Resetting to local mode.')
            self.write_config({'mode': 'local'})
            return True

        return False

    def upload_resource(self, filepath, container, asset_id):
        """

        :param filepath: str path to resource to upload
        :param container: str name of inner container to upload to
        :param asset_id: str name of asset
        :return: str location of uploaded resource
        """
        az_blob_name = '{}/{}'.format(container, asset_id)
        bbs = BlockBlobService(account_name=self.az_account_name,
                               account_key=self.az_account_key)
        bbs.create_container(self.az_container_name)
        bbs.create_blob_from_path(self.az_container_name, az_blob_name, filepath)
        return 'wasbs://{}@{}.blob.core.windows.net/' \
               '{}'.format(self.az_container_name, self.az_account_name, az_blob_name)

    def upload_dependency_to_azure_blob(self, filepath, container, asset_id,
                                        content_type='application/octet-stream'):
        """

        :param filepath: str path to resource to upload
        :param container: str name of inner container to upload to
        :param asset_id: str name of asset
        :param content_type: str content mime type
        :return: str sas url to uploaded dependency
        """
        bbs = BlockBlobService(account_name=self.az_account_name,
                               account_key=self.az_account_key)
        bbs.create_container(container)
        bbs.create_blob_from_path(container, asset_id, filepath,
                                  content_settings=ContentSettings(
                                      content_type=content_type))
        blob_sas = bbs.generate_blob_shared_access_signature(
            container_name=container,
            blob_name=asset_id,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(days=30)
        )
        return 'http://{}.blob.core.windows.net/' \
               '{}/{}?{}'.format(self.az_account_name, container, asset_id, blob_sas)

    @staticmethod
    def cache_local_resource(filepath, container, asset_id):
        """

        :param filepath: str path to resource to upload
        :param container: str name of inner container to upload to
        :param asset_id: str name of asset
        :return: str location of cached resource
        """

        # create a cached version of the asset
        dest_dir = os.path.join(get_home_dir(), '.azuremlcli', container)
        if os.path.exists(dest_dir):
            if not os.path.isdir(dest_dir):
                raise ValueError('Expected asset container {} to be a directory if it'
                                 'exists'.format(dest_dir))
        else:
            try:
                os.makedirs(dest_dir)
            except OSError as exc:
                raise ValueError('Error creating asset directory {} '
                                 'for asset {}: {}'.format(dest_dir, asset_id, exc))
        dest_filepath = os.path.join(dest_dir, asset_id)
        if os.path.isfile(filepath):
            shutil.copyfile(filepath, dest_filepath)
        elif os.path.isdir(filepath):
            shutil.copytree(filepath, dest_filepath)
        else:
            raise ValueError('Assets must be a file or directory.')
        return dest_filepath

    @staticmethod
    def http_call(http_method, url, **kwargs):
        """

        :param http_method: str: (post|get|put|delete)
        :param url: str url to perform http call on
        :return: requests.response object
        """
        http_method = http_method.lower()

        # raises AttributeError if not a valid method
        return getattr(requests, http_method)(url, **kwargs)

    @staticmethod
    def get_args():
        return sys.argv

    @staticmethod
    def os_is_linux():
        return platform.system() in ['Linux', 'linux', 'Unix', 'unix']

    @staticmethod
    def get_input(input_str):
        return input(input_str)

    @staticmethod
    def get_socket(inet, stream):
        return socket.socket(inet, stream)

    @staticmethod
    def check_call(cmd, **kwargs):
        return subprocess.check_call(cmd, **kwargs)


class JupyterContext(CommandLineInterfaceContext):
    def __init__(self):
        super(JupyterContext, self).__init__()
        self.local_mode = True
        self.input_response = {}

    def in_local_mode(self):
        return self.local_mode

    def set_input_response(self, prompt, response):
        self.input_response[prompt] = response

    def get_input(self, prompt):
        return self.input_response[prompt]


# UTILITY FUNCTIONS
def get_json(payload):
    """
    Handles decoding JSON to python objects in py2, py3
    :param payload: str/bytes json to decode
    :return: dict/list/str that represents json
    """
    if isinstance(payload, bytes):
        payload = payload.decode('utf-8')
    return json.loads(payload) if payload else {}


def get_home_dir():
    """
    Function to find home directory on windows or linux environment
    :return: str - path to home directory
    """
    return os.path.expanduser('~')


cli_context = CommandLineInterfaceContext()


def check_version(context, conf):
    """
    :param context: CommandLineInterfaceContext object
    :param conf: dict configuration dictionary
    :return: None
    """
    try:
        output, _ = context.run_cmd('pip search azuremlcli')
        installed_regex = r'INSTALLED:[\s]+(?P<current>[^\s]*)'
        latest_regex = r'LATEST:[\s]+(?P<latest>[^\s]*)'
        latest_search = re.search(latest_regex, output)
        if latest_search:
            installed_search = re.search(installed_regex, output)
            if installed_search:
                print('\033[93mYou are using AzureML CLI version {}, '
                      'but version {} is available.'.format(
                    installed_search.group('current'), latest_search.group('latest')))
                print("You should consider upgrading via the 'pip install --upgrade "
                      "azuremlcli' command.\033[0m")
                print()
        conf['next_version_check'] = (datetime.now() + timedelta(days=1)).strftime(
            '%Y-%m-%d')
        context.write_config(conf)
    except Exception as exc:
        print('Warning: Error determining if there is a newer version of AzureML CLI '
              'available: {}'.format(exc))


def first_run(context):
    """
    Determines if this is the first run (either no config file,
    or config file missing api key). In either case, it prompts
    the user to enter an api key, and validates it. If invalid,
    asks user if they want to continue and add a key at a later
    time. Also sets mode to local if this is the first run.
    Verifies version of CLI as well.
    """

    is_first_run = False
    is_config_corrupt = False
    need_version_check = False
    conf = {}

    try:
        conf = context.read_config()
        if conf:
            try:
                need_version_check = 'next_version_check' not in conf or datetime.now() > datetime.strptime(
                    conf['next_version_check'], '%Y-%m-%d')
            except ValueError:
                is_config_corrupt = True

            if need_version_check:
                check_version(context, conf)
        else:
            is_first_run = True
            conf = {}
    except InvalidConfError:
        is_config_corrupt = True

    if is_config_corrupt:
        print('Warning: Azure ML configuration file is corrupt.')

    if is_first_run or is_config_corrupt:
        conf['mode'] = 'local'
        check_version(context, conf)
        context.write_config(conf)


def get_success_and_resp_str(context, http_response, response_obj=None, verbose=False):
    """

    :param context:
    :param http_response: requests.response object
    :param response_obj: Response object to format a successful response
    :param verbose: bool - flag to increase verbosity
    :return: (bool, str) - (result, result_str)
    """
    if http_response is None:
        return False, "Response was None."
    if verbose:
        print(http_response.content)
    if http_response.status_code == 200:
        try:
            json_obj = get_json(http_response.content)
            if response_obj is not None:
                return True, response_obj.format_successful_response(context, json_obj)
            return True, json.dumps(json_obj, indent=4, sort_keys=True)
        except ValueError:
            return True, http_response.content
    else:
        return False, process_errors(http_response)


def process_errors(http_response):
    """

    :param http_response:
    :return: str message for parsed error
    """
    try:
        json_obj = get_json(http_response.content)
        to_print = '\n'.join(
            [detail['message'] for detail in json_obj['error']['details']])
    except (ValueError, KeyError):
        to_print = http_response.content

    return 'Failed.\nResponse code: {}\n{}'.format(http_response.status_code, to_print)


def validate_remote_filepath(context, filepath):
    """
    Throws exception if remote filepath is invalid.

    :param context: CommandLineInterfaceContext object
    :param filepath: str path to asset file. Should be http or wasb.
    :return: None
    """
    if context.in_local_mode():
        raise ValueError('Remote paths ({}) are not supported in local mode. '
                         'Please specify a local path.'.format(filepath))

    # note - wasb[s]:/// indicates to HDI cluster to use default storage backing
    if filepath.startswith('wasb:///') or filepath.startswith('wasbs:///'):
        return
    http_regex = r'https?://(?P<storage_acct>[^\.]+)\.blob\.core\.windows\.net'
    wasb_regex = r'wasbs?://[^@]+@(?P<storage_acct>[^\.]+)\.blob\.core\.windows\.net'
    for regex in (http_regex, wasb_regex):
        match_obj = re.match(regex, filepath)
        if match_obj and match_obj.group('storage_acct') == context.az_account_name:
            return

    raise ValueError('Remote paths ({}) must be on the backing '
                     'storage ({})'.format(filepath, context.az_account_name))


def update_asset_path(context, verbose, filepath, container, is_input=True):
    """

    :param context: CommandLineInterfaceContext object
    :param verbose: bool True => Debug messages
    :param filepath: str path to asset file. Can be http, wasb, or local file
    :param container: str name of the container to upload to (azureml/$(container)/assetID)
    :param is_input: bool True if asset will be used as an input
    :return: (str, str) (asset_id, location)
    """

    asset_id = os.path.split(filepath)[1]

    if filepath.startswith('http') or filepath.startswith('wasb'):
        validate_remote_filepath(context, filepath)

        # return remote resources as is
        return asset_id, filepath

    # convert relative paths
    filepath = os.path.abspath(os.path.expanduser(filepath))

    # verify that file exists
    if is_input and not os.path.exists(filepath):
        raise ValueError('{} does not exist or is not accessible'.format(filepath))

    if context.in_local_mode():
        if is_input:
            filepath = context.cache_local_resource(filepath, container, asset_id)

        return asset_id, urljoin('file:', pathname2url(filepath))

    if not is_input:
        raise ValueError('Local output paths ({}) are not supported in remote mode. '
                         'Please use a https or wasbs path on the backing '
                         'storage ({})'.format(filepath, context.az_account_name))

    if verbose:
        print('filepath: {}'.format(filepath))
        print('container: {}'.format(container))

    if os.path.isfile(filepath):
        return upload_resource(context, filepath, container, asset_id, verbose)
    elif os.path.isdir(filepath):
        return upload_directory(context, filepath, container, verbose)

    raise ValueError('Resource uploads are only supported for files and directories.')


def upload_directory(context, filepath, container, verbose):
    """

    :param context: CommandLineInterfaceContext object
    :param filepath: str path to directory to upload
    :param container: str name of container to upload to
    :param verbose: bool flag to increase verbosity
    :return: (str, str) (asset_id, location)
    """
    wasb_path = None
    to_strip = os.path.split(filepath)[0]

    for dirpath, _, files in os.walk(filepath):
        for walk_fp in files:
            to_upload = os.path.join(dirpath, walk_fp)
            container_for_upload = '{}/{}'.format(container, to_upload[
                                                             len(to_strip) + 1:-(
                                                             len(walk_fp) + 1)].replace(
                '\\', '/'))
            _, wasb_path = upload_resource(context, to_upload, container_for_upload,
                                           walk_fp,
                                           verbose)

    if wasb_path is None:
        raise ValueError('Directory {} was empty.'.format(filepath))

    asset_id = os.path.basename(filepath)
    match_obj = re.match(r'(?P<wasb_path>.*{})'.format(os.path.basename(filepath)),
                         wasb_path)
    if match_obj:
        return asset_id, match_obj.group('wasb_path')
    raise ValueError('Unable to parse upload location.')


def upload_resource(context, filepath, container, asset_id, verbose):
    """
    Function to upload local resource to blob storage
    :param context: CommandLineInterfaceContext object
    :param filepath: str path of file to upload
    :param container: str name of subcontainer inside azureml container
    :param asset_id: str name of asset inside subcontainer
    :param verbose: bool verbosity flag
    :return: str, str : uploaded asset id, blob location
    """
    wasb_package_location = context.upload_resource(filepath, container, asset_id)
    if verbose:
        print("Asset {} uploaded to {}".format(filepath, wasb_package_location))
    return asset_id, wasb_package_location


def traverse_json(json_obj, traversal_tuple):
    """
        Example:
            {
                "ID": "12345",
                "Properties" {
                    "Name": "a_service"
                }
            }

            If we wanted the "Name" property of the above json to be displayed, we would use the traversal_tuple
                ("Properties", "Name")

        NOTE that list traversal is not supported here, but can work in the case that
        a valid numerical index is passed in the tuple

    :param json_obj: json_obj to traverse. nested dictionaries--lists not supported
    :param traversal_tuple: tuple of keys to traverse the json dict
    :return: string value to display
    """
    trav = json_obj
    for key in traversal_tuple:
        trav = trav[key]
    return trav


class Response(object):  # pylint: disable=too-few-public-methods
    """
    Interface for use constructing response strings from json object for successful requests
    """

    def format_successful_response(self, context, json_obj):
        """

        :param context:
        :param json_obj: json object from successful response
        :return: str response to print to user
        """
        raise NotImplementedError('Class does not implement format_successful_response')


class StaticStringResponse(Response):  # pylint: disable=too-few-public-methods
    """
    Class for use constructing responses that are a static string for successful requests.
    """

    def __init__(self, static_string):
        self.static_string = static_string

    def format_successful_response(self, context, json_obj):
        """

        :param context:
        :param json_obj: json object from successful response
        :return: str response to print to user
        """
        return self.static_string


class TableResponse(Response):
    """
    Class for use constructing response tables from json object for successful requests
    """

    def __init__(self, header_to_value_fn_dict):
        """

        :param header_to_value_fn_dict: dictionary that maps header (str) to a tuple that defines how to
        traverse the json object returned from the service
        """
        self.header_to_value_fn_dict = header_to_value_fn_dict

    def create_row(self, context, json_obj, headers):
        """

        :param json_obj: list or dict to present as table
        :param headers: list of str: headers of table
        :return:
        """
        return [self.header_to_value_fn_dict[header].set_json(json_obj).evaluate(context)
                for header in headers]

    def format_successful_response(self, context, json_obj):
        """

        :param context:
        :param json_obj: list or dict to present as table
        :return: str response to print to user
        """
        rows = []
        headers = self.header_to_value_fn_dict.keys()
        if isinstance(json_obj, list):
            for inner_obj in json_obj:
                rows.append(self.create_row(context, inner_obj, headers))
        else:
            rows.append(self.create_row(context, json_obj, headers))
        return tabulate(rows, headers=[header.upper() for header in headers],
                        tablefmt='psql')


class MultiTableResponse(TableResponse):
    """
    Class for use building responses with multiple tables
    """

    def __init__(self,
                 header_to_value_fn_dicts):  # pylint: disable=super-init-not-called
        """

        :param header_to_value_fn_dicts:
        """

        self.header_to_value_fn_dicts = header_to_value_fn_dicts

    def format_successful_response(self, context, json_obj):
        result = ''
        for header_to_value_fn_dict in self.header_to_value_fn_dicts:
            self.header_to_value_fn_dict = header_to_value_fn_dict
            result += super(MultiTableResponse, self).format_successful_response(context,
                                                                                 json_obj)
            result += '\n'
        return result


class StaticStringWithTableReponse(TableResponse):
    """
    Class for use constructing response that is a static string and tables from json object for successful requests
    """

    def __init__(self, static_string, header_to_value_fn_dict):
        """
        :param static_string: str that will be printed after table
        :param header_to_value_fn_dict: dictionary that maps header (str) to a tuple that defines how to
        traverse the json object returned from the service
        """
        super(StaticStringWithTableReponse, self).__init__(header_to_value_fn_dict)
        self.static_string = static_string

    def format_successful_response(self, context, json_obj):
        return '\n\n'.join([super(StaticStringWithTableReponse,
                                  self).format_successful_response(context, json_obj),
                            self.static_string])


class ValueFunction(object):
    """
    Abstract class for use finding the appropriate value for a given property in a json response.
         defines set_json, a function for storing the json response we will format
         declares evaluate, a function for retrieving the formatted string
    """

    def __init__(self):
        self.json_obj = None

    def set_json(self, json_obj):
        """

        :param json_obj: list or dict to store for processing
        :return: ValueFunction the "self" object with newly updated json_obj member
        """
        self.json_obj = json_obj
        return self

    def evaluate(self, context):
        """

        :param context:
        :return: str value to display
        """
        raise NotImplementedError("Class does not implement evaluate method.")


class TraversalFunction(ValueFunction):
    """
    ValueFunction that consumes a traversal tuple to locate the appropriate string for display
        Example:
            {
                "ID": "12345",
                "Properties" {
                    "Name": "a_service"
                }
            }

            If we wanted the "Name" property of the above json to be displayed, we would use the traversal_tuple
                ("Properties", "Name")

        NOTE that list traversal is not supported here.
    """

    def __init__(self, tup):
        super(TraversalFunction, self).__init__()
        self.traversal_tup = tup

    def evaluate(self, context):
        return traverse_json(self.json_obj, self.traversal_tup)


class ConditionalListTraversalFunction(TraversalFunction):
    """
    Class for use executing actions on members of a list that meet certain criteria
    """

    def __init__(self, tup, condition, action):
        super(ConditionalListTraversalFunction, self).__init__(tup)
        self.condition = condition
        self.action = action

    def evaluate(self, context):
        json_list = super(ConditionalListTraversalFunction, self).evaluate(context)
        return ', '.join(
            [self.action(item) for item in json_list if self.condition(item)])


def is_int(int_str):
    """

    Check whether the given variable can be cast to int
    :param int_str: the variable to check
    :return: bool
    """
    try:
        int(int_str)
        return True
    except ValueError:
        return False


def create_ssh_key_if_not_exists():
    from ._az_util import AzureCliError
    ssh_dir = os.path.join(os.path.expanduser('~'), '.ssh')
    private_key_path = os.path.join(ssh_dir, 'acs_id_rsa')
    public_key_path = '{}.pub'.format(private_key_path)
    if not os.path.exists(private_key_path):
        if not os.path.exists(ssh_dir):
            os.makedirs(ssh_dir, 0o700)
        print('Creating ssh key {}'.format(private_key_path))
        private_key, public_key = generate_ssh_keys()
        with open(private_key_path, 'wb') as private_key_file:
            private_key_file.write(private_key)
        with open(public_key_path, 'wb') as public_key_file:
            public_key_file.write(public_key)
        os.chmod(private_key_path, 0o600)
        os.chmod(public_key_path, 0o600)
        return private_key_path, public_key.decode('ascii')

    try:
        with open(public_key_path, 'r') as sshkeyfile:
            ssh_public_key = sshkeyfile.read().rstrip()
    except IOError:
        try:
            with open(private_key_path, 'rb') as private_key_file:
                key = crypto_serialization.load_pem_private_key(
                    private_key_file.read(),
                    password=None,
                    backend=crypto_default_backend())
            ssh_public_key = key.public_key().public_bytes(
                crypto_serialization.Encoding.OpenSSH,
                crypto_serialization.PublicFormat.OpenSSH
            ).decode('ascii')

        except IOError:
            print('Could not load your SSH public key from {}'.format(public_key_path))
            print('Please run az ml env setup again to create a new ssh keypair.')
            raise AzureCliError('')

    return private_key_path, ssh_public_key


def generate_ssh_keys():
    key = rsa.generate_private_key(
        backend=crypto_default_backend(),
        public_exponent=65537,
        key_size=2048
    )
    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.TraditionalOpenSSL,
        crypto_serialization.NoEncryption())
    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH,
        crypto_serialization.PublicFormat.OpenSSH
    )
    return private_key, public_key
