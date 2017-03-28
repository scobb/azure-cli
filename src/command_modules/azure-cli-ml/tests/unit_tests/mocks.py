import os
import json

try:
    # python 3
    from urllib.request import pathname2url
    from urllib.parse import urljoin, urlparse  # pylint: disable=unused-import
except ImportError:
    # python 2
    from urllib import pathname2url
    from urlparse import urljoin, urlparse

import azure.cli.command_modules.ml._util as cli_util


class TestContext(cli_util.CommandLineInterfaceContext):

    az_account_name = ''
    az_account_key = ''
    acs_master_url = ''
    acs_agent_url = ''
    acr_home = ''
    acr_user = ''
    acr_pw = ''
    hdi_home = ''
    hdi_user = ''
    hdi_pw = ''

    def __init__(self):
        super(TestContext, self).__init__()
        self._cmd_results = {}
        self._cmd_count = {}
        self._config = {}
        self._uploaded_resources = {}
        self._cached_resources = {}
        self._local_mode = False
        self._expected_http_requests = {}

    def set_local_mode(self, mode):
        self._local_mode = mode

    def set_cmd_result(self, cmd, result):
        self._cmd_count[cmd] = 0
        self._cmd_results[cmd] = result

    def set_config(self, config):
        self._config = config

    def run_cmd(self, cmd):
        self._cmd_count[cmd] += 1
        if isinstance(self._cmd_results[cmd], Exception):
            raise self._cmd_results[cmd]
        return self._cmd_results[cmd]

    def get_cmd_count(self, cmd):
        return self._cmd_count[cmd]

    def read_config(self):
        return self._config

    def write_config(self, config):
        self._config = config

    def in_local_mode(self):
        return self._local_mode

    def upload_resource(self, filepath, container, asset_id):
        if not os.path.isfile(filepath) and not os.path.isdir(filepath):
            raise ValueError('Assets must be a file or directory.')

        wasb_location = 'wasb://{}@mywasbstorage.blob.core.windows.net/{}/{}'.format(
            self.az_container_name,
            container,
            os.path.basename(filepath))
        self._uploaded_resources[asset_id] = wasb_location
        return wasb_location

    def cache_local_resource(self, filepath, container, asset_id):
        if not os.path.isfile(filepath) and not os.path.isdir(filepath):
            raise ValueError('Assets must be a file or directory.')
        self._cached_resources[asset_id] = urljoin('file:', pathname2url(filepath))
        return filepath

    def get_uploaded_resources(self):
        return self._uploaded_resources

    def get_cached_resources(self):
        return self._cached_resources

    def http_call(self, method, url, **kwargs):
        entry_key = '{} {}'.format(method.lower(), url)
        if entry_key not in self._expected_http_requests:
            raise ValueError('Unexpected method called: {}'.format(entry_key))
        if isinstance(self._expected_http_requests[entry_key], Exception):
            raise self._expected_http_requests[entry_key]
        return self._expected_http_requests[entry_key]

    def set_expected_http_response(self, method, url, response):
        self._expected_http_requests['{} {}'.format(method.lower(), url)] = response


class CorruptConfigTestContext(TestContext):
    def read_config(self):
        if self._config:
            return self._config
        raise cli_util.InvalidConfError


class MockHttpResponse(object):
    def __init__(self, content, status_code):
        self.content = content
        self.status_code = status_code

    def json(self):
        return json.loads(self.content)


class MockResponse(cli_util.Response):
    def format_successful_response(self, context, json_obj):
        """

        :param json_obj: json object from successful response
        :return: str response to print to user
        """
        return json.dumps(json_obj)


class MockSocket(object):
    def __init__(self, port):
        self.tup_list = None
        self.port = port

    def bind(self, tup_list):
        self.tup_list = tup_list

    def getsockname(self):
        """

        :return: (str, int): (address, port)
        """
        return '0.0.0.0', self.port

    def close(self):
        pass


class MockProcess(object):
    def __init__(self, output, err):
        self.output = output
        self.err = err

    def communicate(self):
        return self.output, self.err


class MockProfile(object):
    pass