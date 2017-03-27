import unittest
from mock import call
from mock import mock_open
from mock import patch
import sys
import requests
import json
import os
import subprocess
from azure.cli.command_modules.ml._az_util import AzureCliError
from azure.cli.command_modules.ml._az_util import InvalidNameError
# from azure.cli.command_modules.ml._az_util import SshKeygenError
from azure.cli.command_modules.ml._util import InvalidConfError
from .mocks import TestContext
from .mocks import MockHttpResponse
from .mocks import MockSocket
# from azure.cli.command_modules.ml.env import env
from azure.cli.command_modules.ml.env import env_describe
from azure.cli.command_modules.ml.env import env_setup
from azure.cli.command_modules.ml.env import env_local
from azure.cli.command_modules.ml.env import env_cluster
from azure.cli.command_modules.ml.env import validate_acs_marathon
from azure.cli.command_modules.ml.env import acs_marathon_setup
# from azure.cli.command_modules.ml.env import report_acs_success
# from azure.cli.command_modules.ml.env import validate_and_read_ssh_keys
# from azure.cli.command_modules.ml.env import validate_and_create_storage
# from azure.cli.command_modules.ml.env import validate_and_create_acr
# from azure.cli.command_modules.ml.env import validate_and_create_acs


class EnvUnitTests(unittest.TestCase):
    amlenvrc_path = os.path.join(os.path.expanduser('~'), '.amlenvrc')
    ssh_folder = os.path.join(os.path.expanduser('~'), '.ssh')
    rsa_private = os.path.join(ssh_folder, 'id_rsa')
    rsa_public = rsa_private + '.pub'
    env_describe_local_string = ('** Warning: Running in local mode. **\n'
                                 'To switch to cluster mode: aml env cluster\n\n'
                                 'Storage account name   : None\n'
                                 'Storage account key    : None\n'
                                 'ACR URL                : None\n'
                                 'ACR username           : None\n'
                                 'ACR password           : None')

    env_describe_cluster_string = ('Storage account name   : None\n'
                                   'Storage account key    : None\n'
                                   'ACR URL                : None\n'
                                   'ACR username           : None\n'
                                   'ACR password           : None\n'
                                   'HDI cluster URL        : None\n'
                                   'HDI admin user name    : None\n'
                                   'HDI admin password     : None\n'
                                   'ACS Master URL         : {}\n'
                                   'ACS Agent URL          : None\n'
                                   'ACS Port forwarding    : {}')

    def test_env_describe_local_mode(self):
        context = TestContext()
        context.set_local_mode(True)
        env_describe(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, self.env_describe_local_string)

    def test_env_describe_cluster_mode(self):
        context = TestContext()
        context.set_local_mode(False)
        env_describe(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, self.env_describe_cluster_string.format('None', 'OFF'))

    def test_env_describe_cluster_mode_port_forwarding(self):
        context = TestContext()
        context.set_local_mode(False)
        context.set_config({'mode': 'cluster', 'port': '55'})
        env_describe(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         self.env_describe_cluster_string.format('None', 'ON, port 55'))

    def test_env_local_bad_arg(self):
        context = TestContext()
        args = ['-b']
        env_local(context, args)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'aml env local [-v]')

    def test_env_local_windows(self):
        context = TestContext()
        context.set_os_name('Windows')
        env_local(context, [])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Local mode is not supported on this platform.')

    def test_env_local_happy(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.set_config({'mode': 'cluster'})
        env_local(context, [])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(context.in_local_mode())
        self.assertEqual(output, self.env_describe_local_string)

    def test_env_local_happy_verbose(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.set_config({'mode': 'cluster'})
        env_local(context, [])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(context.in_local_mode())
        self.assertEqual(output, self.env_describe_local_string)

    def test_env_local_no_config(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.set_config(None)
        env_local(context, [])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(context.in_local_mode())
        self.assertEqual(output, self.env_describe_local_string)

    def test_env_local_no_config_verbose(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.set_config(None)
        env_local(context, args=['-v'])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(context.in_local_mode())
        self.assertEqual(output, '[Debug] No configuration file found.\n\n{}'.format(
            self.env_describe_local_string))

    def test_env_local_no_mode_verbose(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.set_config({'stuff': 'here'})
        env_local(context, args=['-v'])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(context.in_local_mode())
        self.assertEqual(output,
                         '[Debug] No mode setting found in config file. Suspicious.\n\n{}'.format(
                             self.env_describe_local_string))

    def test_env_local_exception_verbose(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.set_config(InvalidConfError())
        env_local(context, args=['-v'])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(context.in_local_mode())
        self.assertEqual(output,
                         '[Debug] Suspicious content in ~/.amlconf.\n[Debug] Resetting.\n\n{}'.format(
                             self.env_describe_local_string))

    def test_validate_acs_marathon_existing_port_happy(self):
        context = TestContext()
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           MockHttpResponse(
                                               json.dumps({'name': 'marathon',
                                                           'version': 'blah'}), 200))
        is_set_up, port = validate_acs_marathon(context, 15)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Successfully tested ACS connection. Found marathon endpoint at http://127.0.0.1:15/marathon/v2')
        self.assertTrue(is_set_up)
        self.assertEqual(port, 15)

    def test_validate_acs_marathon_config_port_happy(self):
        context = TestContext()
        context.set_config({'mode': 'cluster', 'port': 15})
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           MockHttpResponse(
                                               json.dumps({'name': 'marathon',
                                                           'version': 'blah'}), 200))
        is_set_up, port = validate_acs_marathon(context, -1)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Successfully tested ACS connection. Found marathon endpoint at http://127.0.0.1:15/marathon/v2')
        self.assertTrue(is_set_up)
        self.assertEqual(port, 15)

    def test_validate_acs_marathon_direct_connection_happy(self):
        context = TestContext()
        context.acs_master_url = 'test.master.com'
        context.set_expected_http_response('get',
                                           'http://test.master.com/marathon/v2/info',
                                           MockHttpResponse(
                                               json.dumps({'name': 'marathon',
                                                           'version': 'blah'}), 200))
        is_set_up, port = validate_acs_marathon(context, 0)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Trying direct connection to ACS cluster at http://test.master.com/marathon/v2\n'
                         'Successfully tested ACS connection. Found marathon endpoint at http://test.master.com/marathon/v2')
        self.assertTrue(is_set_up)
        self.assertEqual(port, 0)

    def test_validate_acs_marathon_config_direct_connection_happy(self):
        context = TestContext()
        context.set_config({'mode': 'cluster', 'port': 0})
        context.acs_master_url = 'test.master.com'
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           requests.ConnectionError())
        context.set_expected_http_response('get',
                                           'http://test.master.com/marathon/v2/info',
                                           MockHttpResponse(
                                               json.dumps({'name': 'marathon',
                                                           'version': 'blah'}), 200))
        is_set_up, port = validate_acs_marathon(context, 15)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Marathon endpoint not available at http://127.0.0.1:15/marathon/v2\n'
                         'Found previous direct connection to ACS cluster. Checking if it still works.\n'
                         'Trying direct connection to ACS cluster at http://test.master.com/marathon/v2\n'
                         'Successfully tested ACS connection. Found marathon endpoint at http://test.master.com/marathon/v2')
        self.assertTrue(is_set_up)
        self.assertEqual(port, 0)

    def test_validate_acs_marathon_port_unhappy_config_port_happy(self):
        context = TestContext()
        context.set_config({'mode': 'cluster', 'port': 16})
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           requests.ConnectionError())
        context.set_expected_http_response('get', 'http://127.0.0.1:16/marathon/v2/info',
                                           MockHttpResponse(
                                               json.dumps({'name': 'marathon',
                                                           'version': 'blah'}), 200))
        is_set_up, port = validate_acs_marathon(context, 15)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Marathon endpoint not available at http://127.0.0.1:15/marathon/v2\n'
                         'Found previous port forwarding set up at 16. Checking if it still works.\n'
                         'Successfully tested ACS connection. Found marathon endpoint at http://127.0.0.1:16/marathon/v2')
        self.assertTrue(is_set_up)
        self.assertEqual(port, 16)

    def test_validate_acs_marathon_existing_port_unhappy_no_config_port_windows(self):
        context = TestContext()
        context.set_os_name('Windows')
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           requests.ConnectionError())
        is_set_up, port = validate_acs_marathon(context, 15)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Marathon endpoint not available at http://127.0.0.1:15/marathon/v2\n'
                         'Unable to automatically set port forwarding for Windows machines.')
        self.assertFalse(is_set_up)
        self.assertEqual(port, -1)

    def test_validate_acs_marathon_existing_port_unhappy_no_config_port_local_setup_windows(
            self):
        context = TestContext()
        context.set_os_name('Windows')
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           requests.ConnectionError())
        context.set_expected_http_response('get',
                                           'http://test.acs.master/marathon/v2/info',
                                           requests.ConnectionError())
        context.acs_master_url = 'test.acs.master'
        context.set_config({'port': '0'})
        is_set_up, port = validate_acs_marathon(context, 15)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Marathon endpoint not available at http://127.0.0.1:15/marathon/v2\n'
                         'Found previous direct connection to ACS cluster. Checking if it still works.\n'
                         'Trying direct connection to ACS cluster at http://test.acs.master/marathon/v2\n'
                         'Marathon endpoint not available at http://test.acs.master/marathon/v2')
        self.assertFalse(is_set_up)
        self.assertEqual(port, -1)

    def test_validate_acs_marathon_existing_port_bad_json_windows(self):
        context = TestContext()
        context.set_os_name('Windows')
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           MockHttpResponse('stuff', 200))
        is_set_up, port = validate_acs_marathon(context, 15)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Marathon endpoint not available at http://127.0.0.1:15/marathon/v2\nUnable to automatically set port forwarding for Windows machines.')
        self.assertFalse(is_set_up)
        self.assertEqual(port, -1)

    def test_validate_acs_marathon_existing_port_invalid_marathon_response_windows(self):
        context = TestContext()
        context.set_os_name('Windows')
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           requests.ConnectionError())
        context.set_expected_http_response('get', 'http://127.0.0.1:16/marathon/v2/info',
                                           MockHttpResponse(json.dumps('stuff'), 200))
        context.set_config({'mode': 'cluster', 'port': 16})
        is_set_up, port = validate_acs_marathon(context, 15)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Marathon endpoint not available at http://127.0.0.1:15/marathon/v2\n'
                         'Found previous port forwarding set up at 16. Checking if it still works.\n'
                         'Marathon endpoint not available at http://127.0.0.1:16/marathon/v2\n'
                         'Unable to automatically set port forwarding for Windows machines.')
        self.assertFalse(is_set_up)
        self.assertEqual(port, -1)

    def test_validate_acs_marathon_no_port_forwarding_value_err(self):
        context = TestContext()
        context.set_os_name('Windows')
        context.set_expected_http_response('get',
                                           'http://test.acs.master/marathon/v2/info',
                                           MockHttpResponse('stuff', 200))
        context.acs_master_url = 'test.acs.master'
        is_set_up, port = validate_acs_marathon(context, 0)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Trying direct connection to ACS cluster at http://test.acs.master/marathon/v2\n'
                         'Marathon endpoint not available at http://test.acs.master/marathon/v2')
        self.assertFalse(is_set_up)
        self.assertEqual(port, -1)

    def test_validate_acs_marathon_no_port_forwarding_missing_key(self):
        context = TestContext()
        context.set_os_name('Windows')
        context.set_expected_http_response('get',
                                           'http://test.acs.master/marathon/v2/info',
                                           MockHttpResponse(json.dumps(['a_list']), 200))
        context.acs_master_url = 'test.acs.master'
        is_set_up, port = validate_acs_marathon(context, 0)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Trying direct connection to ACS cluster at http://test.acs.master/marathon/v2\n'
                         'Marathon endpoint not available at http://test.acs.master/marathon/v2')
        self.assertFalse(is_set_up)
        self.assertEqual(port, -1)

    def test_validate_acs_marathon_no_port_forwarding_missing_url(self):
        context = TestContext()
        context.set_os_name('Windows')
        context.acs_master_url = None
        is_set_up, port = validate_acs_marathon(context, 0)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, '')
        self.assertFalse(is_set_up)
        self.assertEqual(port, -1)

    def test_validate_acs_marathon_first_run_windows(self):
        context = TestContext()
        context.set_os_name('Windows')
        context.acs_master_url = None
        is_set_up, port = validate_acs_marathon(context, -1)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unable to automatically set port forwarding for Windows machines.')
        self.assertFalse(is_set_up)
        self.assertEqual(port, -1)

    def test_validate_acs_marathon_first_run_linux(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.acs_master_url = 'test.acs.url'
        context.set_input('Enter ACS Master URL (default: {}): '.format(
            context.acs_master_url), 'my.other.acs')
        context.set_input('Enter ACS username (default: acsadmin): ', 'testuser')
        context.set_socket(MockSocket(15))
        cmd = ['ssh', '-L', '{}:localhost:80'.format(15),
               '-f', '-N', '{}@{}'.format('testuser',
                                          'my.other.acs'), '-p',
               '2200']
        context.set_cmd_result(cmd, '')
        is_setup, port = validate_acs_marathon(context, -1)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Establishing connection to ACS cluster.\n"
                                 "Forwarding local port 15 to port 80 on your ACS cluster")
        self.assertTrue(is_setup)
        self.assertEqual(port, 15)
        self.assertEqual(context.get_cmd_count(' '.join(cmd)), 1)

    def test_acs_marathon_setup_happy_defaults(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.acs_master_url = 'test.acs.url'
        context.set_input('Enter ACS Master URL (default: {}): '.format(
            context.acs_master_url), '')
        context.set_input('Enter ACS username (default: acsadmin): ', '')
        context.set_socket(MockSocket(15))
        cmd = ['ssh', '-L', '{}:localhost:80'.format(15),
               '-f', '-N', '{}@{}'.format('acsadmin',
                                          context.acs_master_url), '-p',
               '2200']
        context.set_cmd_result(cmd, '')
        is_setup, port = acs_marathon_setup(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Establishing connection to ACS cluster.\n"
                                 "Forwarding local port 15 to port 80 on your ACS cluster")
        self.assertTrue(is_setup)
        self.assertEqual(port, 15)
        self.assertEqual(context.get_cmd_count(' '.join(cmd)), 1)

    def test_acs_marathon_setup_happy_inputs(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.acs_master_url = 'test.acs.url'
        context.set_input('Enter ACS Master URL (default: {}): '.format(
            context.acs_master_url), 'my.other.acs')
        context.set_input('Enter ACS username (default: acsadmin): ', 'testuser')
        context.set_socket(MockSocket(15))
        cmd = ['ssh', '-L', '{}:localhost:80'.format(15),
               '-f', '-N', '{}@{}'.format('testuser',
                                          'my.other.acs'), '-p',
               '2200']
        context.set_cmd_result(cmd, '')
        is_setup, port = acs_marathon_setup(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Establishing connection to ACS cluster.\n"
                                 "Forwarding local port 15 to port 80 on your ACS cluster")
        self.assertTrue(is_setup)
        self.assertEqual(port, 15)
        self.assertEqual(context.get_cmd_count(' '.join(cmd)), 1)

    def test_acs_marathon_setup_no_acs_no_default(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.set_input('Enter ACS Master URL (default: {}): '.format(
            context.acs_master_url), '')

        is_setup, port = acs_marathon_setup(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Establishing connection to ACS cluster.\n"
                                 "Error: no ACS URL provided.")
        self.assertFalse(is_setup)
        self.assertEqual(port, -1)

    def test_acs_marathon_setup_called_process_error(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.acs_master_url = 'test.acs.url'
        context.set_input('Enter ACS Master URL (default: {}): '.format(
            context.acs_master_url), 'my.other.acs')
        context.set_input('Enter ACS username (default: acsadmin): ', 'testuser')
        context.set_socket(MockSocket(15))
        cmd = ['ssh', '-L', '{}:localhost:80'.format(15),
               '-f', '-N', '{}@{}'.format('testuser',
                                          'my.other.acs'), '-p',
               '2200']
        context.set_cmd_result(cmd, subprocess.CalledProcessError(2, cmd))
        is_setup, port = acs_marathon_setup(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Establishing connection to ACS cluster.\n'
                                 'Forwarding local port 15 to port 80 on your ACS cluster\n'
                                 'Failed to set up ssh tunnel. Error code: 2')
        self.assertFalse(is_setup)
        self.assertEqual(port, -1)
        self.assertEqual(context.get_cmd_count(' '.join(cmd)), 1)

    def test_env_cluster_force_no_conf(self):
        context = TestContext()
        context.acs_master_url = 'test.master.com'
        context.set_expected_http_response('get',
                                           'http://test.master.com/marathon/v2/info',
                                           MockHttpResponse(
                                               json.dumps({'name': 'marathon',
                                                           'version': 'blah'}), 200))
        args = ['-f']
        env_cluster(context, args)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Trying direct connection to ACS cluster at http://test.master.com/marathon/v2\n'
                         'Successfully tested ACS connection. Found marathon endpoint at http://test.master.com/marathon/v2\n'
                         'Running in cluster mode.\n'
                         '{}'.format(self.env_describe_cluster_string.format(
                             context.acs_master_url, 'OFF')))
        conf = context.read_config()
        self.assertEqual(conf['mode'], 'cluster')
        self.assertEqual(conf['port'], 0)

    def test_env_cluster_port(self):
        context = TestContext()
        context.set_expected_http_response('get', 'http://127.0.0.1:15/marathon/v2/info',
                                           MockHttpResponse(
                                               json.dumps({'name': 'marathon',
                                                           'version': 'blah'}), 200))
        args = ['-p', '15']
        env_cluster(context, args)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Successfully tested ACS connection. Found marathon endpoint at http://127.0.0.1:15/marathon/v2\n'
                         'Running in cluster mode.\n'
                         '{}'.format(self.env_describe_cluster_string.format('None',
                                                                             'ON, port 15')))
        conf = context.read_config()
        self.assertEqual(conf['mode'], 'cluster')
        self.assertEqual(conf['port'], 15)

    def test_env_cluster_no_port(self):
        context = TestContext()
        context.set_os_name('Linux')
        context.acs_master_url = 'test.acs.url'
        context.set_input('Enter ACS Master URL (default: {}): '.format(
            context.acs_master_url), '')
        context.set_input('Enter ACS username (default: acsadmin): ', '')
        context.set_socket(MockSocket(15))
        cmd = ['ssh', '-L', '{}:localhost:80'.format(15),
               '-f', '-N', '{}@{}'.format('acsadmin',
                                          context.acs_master_url), '-p',
               '2200']
        context.set_cmd_result(cmd, '')

        args = ['-p']
        env_cluster(context, args)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Establishing connection to ACS cluster.\n'
                                 'Forwarding local port 15 to port 80 on your ACS cluster\n'
                                 'Running in cluster mode.\n'
                                 '{}'.format(
            self.env_describe_cluster_string.format(context.acs_master_url,
                                                    'ON, port 15')))
        conf = context.read_config()
        self.assertEqual(conf['mode'], 'cluster')
        self.assertEqual(conf['port'], 15)

    def test_env_cluster_no_port_invalid_config(self):
        context = TestContext()
        context.set_config(InvalidConfError())
        context.set_os_name('Linux')
        context.acs_master_url = 'test.acs.url'
        context.set_input('Enter ACS Master URL (default: {}): '.format(
            context.acs_master_url), '')
        context.set_input('Enter ACS username (default: acsadmin): ', '')
        context.set_socket(MockSocket(15))
        cmd = ['ssh', '-L', '{}:localhost:80'.format(15),
               '-f', '-N', '{}@{}'.format('acsadmin',
                                          context.acs_master_url), '-p',
               '2200']
        context.set_cmd_result(cmd, '')

        args = ['-p', '-v']
        env_cluster(context, args)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Establishing connection to ACS cluster.\n'
                                 'Forwarding local port 15 to port 80 on your ACS cluster\n'
                                 '[Debug] Suspicious content in ~/.amlconf.\n'
                                 '[Debug] Resetting.\n'
                                 'Running in cluster mode.\n'
                                 '{}'.format(
            self.env_describe_cluster_string.format(context.acs_master_url,
                                                    'ON, port 15')))
        conf = context.read_config()
        self.assertEqual(conf['mode'], 'cluster')
        self.assertEqual(conf['port'], 15)

    def test_env_cluster_force_no_connection_abort(self):
        context = TestContext()
        context.acs_master_url = 'test.master.com'
        context.set_expected_http_response('get',
                                           'http://test.master.com/marathon/v2/info',
                                           requests.ConnectionError())
        args = ['-f']
        context.set_input('Could not connect to ACS cluster. Continue with cluster mode '
                          'anyway (y/N)? ', '')
        env_cluster(context, args)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Trying direct connection to ACS cluster at http://test.master.com/marathon/v2\n'
                         'Marathon endpoint not available at http://test.master.com/marathon/v2\n'
                         "Aborting switch to cluster mode. Please run 'aml env about' for more information on setting up your cluster.")
        conf = context.read_config()
        self.assertFalse('mode' in conf)

    def test_env_setup_no_az(self):
        context = TestContext()
        context.set_cmd_result('az --version', subprocess.CalledProcessError('blah', 1))
        env_setup(context, [])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Couldn't find the Azure CLI installed on the system.\n"
                                 "Please install the Azure CLI by running the following:\n"
                                 "sudo pip install azure-cli")

    def test_env_setup_no_az_trash_output(self):
        context = TestContext()
        context.set_cmd_result('az --version', '-bash: az: command not found')
        env_setup(context, [])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Couldn't find the Azure CLI installed on the system.\n"
                                 "Please install the Azure CLI by running the following:\n"
                                 "sudo pip install azure-cli")

    @patch('azure.cli.command_modules.ml.env.az_check_acs_status')
    def test_env_setup_status_ready(self, m):
        m.return_value = 'test.acs.master', 'test.acs.agent'
        context = TestContext()
        context.set_cmd_result('az --version', 'azure-cli')
        o = mock_open()
        with patch('azure.cli.command_modules.ml.env.open', o, create=True):
            env_setup(context, ['-s', 'an_id'])

        self.assertEqual(o.mock_calls, [call(self.amlenvrc_path, 'a+'),
                                        call().__enter__(),
                                        call().write('set AML_ACS_MASTER=test.acs.master\n'),
                                        call().write('set AML_ACS_AGENT=test.acs.agent\n'),
                                        call().__exit__(None, None, None)])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        m.assert_called_once_with('an_id')
        self.assertEqual(output, "ACS deployment succeeded.\n"
                                 "ACS Master URL     : test.acs.master\n"
                                 "ACS Agent URL      : test.acs.agent\n"
                                 "ACS admin username : acsadmin (Needed to set up port forwarding in cluster mode).\n"
                                 "To configure aml with this environment, set the following environment variables.\n"
                                 " set AML_ACS_MASTER=test.acs.master\n"
                                 " set AML_ACS_AGENT=test.acs.agent\n"
                                 "You can also find these settings saved in {}\n\nTo "
                                 "switch to cluster mode, run 'aml env "
                                 "cluster'.".format(self.amlenvrc_path))

    @patch('azure.cli.command_modules.ml.env.az_check_acs_status')
    def test_env_setup_status_not_ready(self, m):
        m.return_value = None, None
        context = TestContext()
        context.set_cmd_result('az --version', 'azure-cli')
        env_setup(context, ['-s', 'an_id'])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        m.assert_called_once_with('an_id')
        self.assertEqual(output, "")

    @patch('azure.cli.command_modules.ml.env.az_check_acs_status')
    def test_env_setup_status_azure_cli_error(self, m):
        m.return_value = None, None
        m.side_effect = AzureCliError('stuff')
        context = TestContext()
        context.set_cmd_result('az --version', 'azure-cli')
        env_setup(context, ['-s', 'an_id'])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        m.assert_called_once_with('an_id')
        self.assertEqual(output, "stuff")

    @patch('azure.cli.command_modules.ml.env.validate_env_name')
    def test_env_setup_name_invalid_name(self, m):
        m.side_effect = InvalidNameError('It is bad.')
        context = TestContext()
        context.set_cmd_result('az --version', 'azure-cli')
        env_setup(context, ['-n', 'an_invalid_name'])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        m.assert_called_once_with('an_invalid_name')
        self.assertEqual(output, "Invalid environment name. It is bad.")

    # @patch('azure.cli.command_modules.ml.env.validate_and_read_ssh_keys')
    # @patch('azure.cli.command_modules.ml.env.validate_env_name')
    # def test_env_setup_name_keygen_err(self, validate_env_name_mock,
    #                                    validate_and_read_ssh_keys_mock):
    #     validate_and_read_ssh_keys_mock.side_effect = SshKeygenError('An error.')
    #     context = TestContext()
    #     context.set_cmd_result('az --version', 'azure-cli')
    #     env_setup(context, ['-n', 'a_name'])
    #     if not hasattr(sys.stdout, "getvalue"):
    #         self.fail("need to run in buffered mode")
    #     output = sys.stdout.getvalue().strip()
    #     validate_env_name_mock.assert_called_once_with('a_name')
    #     self.assertEqual(output, "")

    @staticmethod
    def update_acr_user_side_effect(context, rn, rg, sa):
        context.acr_user = 'acr_user'
        return 'acr_login_server', 'acr_password'

    @patch('azure.cli.command_modules.ml.env.validate_and_read_ssh_keys')
    @patch('azure.cli.command_modules.ml.env.validate_env_name')
    def test_env_setup_no_name_no_status_invalid_input(self,
                                                       validate_env_name_mock,
                                                       validate_and_read_ssh_keys_mock):
        validate_env_name_mock.side_effect = InvalidNameError('It is bad.')
        validate_and_read_ssh_keys_mock.return_value = 'a_key'
        context = TestContext()
        context.set_cmd_result('az --version', 'azure-cli')
        context.set_input(
            'Enter environment name (1-20 characters, lowercase alphanumeric): ',
            'an_invalid_name')
        env_setup(context, [])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        validate_env_name_mock.assert_called_once_with('an_invalid_name')
        validate_and_read_ssh_keys_mock.assert_called_once_with(context)
        self.assertEqual(output,
                         "Setting up your Azure ML environment with a storage account, ACR registry and ACS cluster.\nInvalid environment name. It is bad.")

    @patch('azure.cli.command_modules.ml.env.az_check_subscription')
    @patch('azure.cli.command_modules.ml.env.validate_and_create_acs')
    @patch('azure.cli.command_modules.ml.env.validate_and_create_acr')
    @patch('azure.cli.command_modules.ml.env.validate_and_read_ssh_keys')
    @patch('azure.cli.command_modules.ml.env.validate_and_create_storage')
    @patch('azure.cli.command_modules.ml.env.az_create_resource_group')
    @patch('azure.cli.command_modules.ml.env.az_login')
    @patch('azure.cli.command_modules.ml.env.validate_env_name')
    def test_env_setup_no_name_no_status_happy(self,
                                               validate_env_name_mock,
                                               az_login_mock,
                                               az_create_resource_group_mock,
                                               validate_and_create_storage_mock,
                                               validate_and_read_ssh_keys_mock,
                                               validate_and_create_acr_mock,
                                               validate_and_create_acs_mock,
                                               az_check_subscription_mock):
        validate_and_read_ssh_keys_mock.return_value = 'a_key'
        az_create_resource_group_mock.return_value = 'a_resource_group'
        validate_and_create_storage_mock.return_value = 'act_name', 'act_key'
        validate_and_create_acr_mock.side_effect = (self.update_acr_user_side_effect)
        context = TestContext()
        context.set_cmd_result('az --version', 'azure-cli')
        context.set_input(
            'Enter environment name (1-20 characters, lowercase alphanumeric): ',
            'a_name')
        m = mock_open()
        with patch('azure.cli.command_modules.ml.env.open', m, create=True):
            env_setup(context, [])
        self.assertEqual(m.mock_calls, [call(self.amlenvrc_path, 'w+'),
                                        call().__enter__(),
                                        call().write(
                                            'set AML_STORAGE_ACCT_NAME=act_name\n'
                                            'set AML_STORAGE_ACCT_KEY=act_key\n'
                                            'set AML_ACR_HOME=acr_login_server\n'
                                            'set AML_ACR_USER=acr_user\n'
                                            'set AML_ACR_PW=acr_password'),
                                        call().__exit__(None, None, None)])
        validate_env_name_mock.assert_called_once_with('a_name')
        validate_and_read_ssh_keys_mock.assert_called_once_with(context)
        validate_and_create_storage_mock.assert_called_once_with(context, 'a_name', 'a_resource_group')
        validate_and_create_acr_mock.assert_called_once_with(context, 'a_name', 'a_resource_group', 'act_name')
        validate_and_create_acs_mock.assert_called_once_with(context, 'a_name', 'a_resource_group', 'acr_login_server',
                                                             'acr_password', 'a_key')

    @patch('azure.cli.command_modules.ml.env.az_check_subscription')
    @patch('azure.cli.command_modules.ml.env.validate_and_create_acs')
    @patch('azure.cli.command_modules.ml.env.validate_and_create_acr')
    @patch('azure.cli.command_modules.ml.env.validate_and_read_ssh_keys')
    @patch('azure.cli.command_modules.ml.env.validate_and_create_storage')
    @patch('azure.cli.command_modules.ml.env.az_create_resource_group')
    @patch('azure.cli.command_modules.ml.env.az_login')
    @patch('azure.cli.command_modules.ml.env.validate_env_name')
    def test_env_setup_no_name_no_status_happy_IOError(self,
                                                       validate_env_name_mock,
                                                       az_login_mock,
                                                       az_create_resource_group_mock,
                                                       validate_and_create_storage_mock,
                                                       validate_and_read_ssh_keys_mock,
                                                       validate_and_create_acr_mock,
                                                       validate_and_create_acs_mock,
                                                       az_check_subscription_mock):
        validate_and_read_ssh_keys_mock.return_value = 'a_key'
        az_create_resource_group_mock.return_value = 'a_resource_group'
        validate_and_create_storage_mock.return_value = 'act_name', 'act_key'
        validate_and_create_acr_mock.side_effect = (self.update_acr_user_side_effect)
        context = TestContext()
        context.set_cmd_result('az --version', 'azure-cli')
        context.set_input(
            'Enter environment name (1-20 characters, lowercase alphanumeric): ',
            'a_name')
        m = mock_open()
        m.side_effect = IOError
        with patch('azure.cli.command_modules.ml.env.open', m, create=True):
            env_setup(context, [])
        self.assertEqual(m.mock_calls, [call(self.amlenvrc_path, 'w+')])
        validate_env_name_mock.assert_called_once_with('a_name')
        validate_and_read_ssh_keys_mock.assert_called_once_with(context)
        validate_and_create_storage_mock.assert_called_once_with(context, 'a_name', 'a_resource_group')
        validate_and_create_acr_mock.assert_called_once_with(context, 'a_name', 'a_resource_group', 'act_name')
        validate_and_create_acs_mock.assert_called_once_with(context, 'a_name', 'a_resource_group', 'acr_login_server',
                                                             'acr_password', 'a_key')

    @patch('azure.cli.command_modules.ml.env.validate_env_name')
    @patch('azure.cli.command_modules.ml.env.validate_and_read_ssh_keys')
    @patch('azure.cli.command_modules.ml.env.az_login')
    def test_env_setup_with_name_no_status_exception(self,
                                                     az_login_mock,
                                                     validate_and_read_ssh_keys_mock,
                                                     validate_env_name_mock):
        validate_and_read_ssh_keys_mock.return_value = 'a_key'
        az_login_mock.side_effect = AzureCliError('an error msg')
        context = TestContext()
        context.set_cmd_result('az --version', 'azure-cli')
        env_setup(context, ['-n', 'a_name'])

        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        validate_and_read_ssh_keys_mock.assert_called_once_with(context)
        self.assertEqual(output,
                         'Setting up your Azure ML environment with a storage account, ACR registry and ACS cluster.\n'
                         'an error msg')

    @patch('azure.cli.command_modules.ml.env.az_create_storage_account')
    def test_validate_and_create_storage_none_set_up(self,
                                                     az_create_storage_account_mock):
        context = TestContext()
        az_create_storage_account_mock.return_value = 'act_name', 'act_key'
        name, key = validate_and_create_storage(context, 'root', 'rg')
        az_create_storage_account_mock.assert_called_once_with(context, 'root', 'rg')
        self.assertEqual(name, 'act_name')
        self.assertEqual(key, 'act_key')

    @patch('azure.cli.command_modules.ml.env.az_create_storage_account')
    def test_validate_and_create_storage_set_up_n(self, az_create_storage_account_mock):
        context = TestContext()
        context.az_account_key = 'existing_key'
        context.az_account_name = 'existing_name'
        context.set_input('Setup a new storage account instead (y/N)?', '')
        az_create_storage_account_mock.return_value = 'act_name', 'act_key'
        name, key = validate_and_create_storage(context, 'root', 'rg')
        az_create_storage_account_mock.assert_not_called()
        self.assertEqual(name, 'existing_name')
        self.assertEqual(key, 'existing_key')

    @patch('azure.cli.command_modules.ml.env.az_create_storage_account')
    def test_validate_and_create_storage_set_up_y(self, az_create_storage_account_mock):
        context = TestContext()
        context.az_account_key = 'existing_key'
        context.az_account_name = 'existing_name'
        context.set_input('Setup a new storage account instead (y/N)?', 'y')
        az_create_storage_account_mock.return_value = 'act_name', 'act_key'
        name, key = validate_and_create_storage(context, 'root', 'rg')
        az_create_storage_account_mock.assert_called_once_with(context, 'root', 'rg')
        self.assertEqual(name, 'act_name')
        self.assertEqual(key, 'act_key')

    @patch('azure.cli.command_modules.ml.env.az_create_acr')
    def test_validate_and_create_acr_none_set_up(self, az_create_acr_mock):
        context = TestContext()
        az_create_acr_mock.return_value = 'server', 'user', 'pw'
        server, pw = validate_and_create_acr(context, 'root', 'rg', 'act')
        az_create_acr_mock.assert_called_once_with(context, 'root', 'rg', 'act')
        self.assertEqual(server, 'server')
        self.assertEqual(pw, 'pw')
        self.assertEqual(context.acr_user, 'user')

    @patch('azure.cli.command_modules.ml.env.az_create_acr')
    def test_validate_and_create_acr_set_up_y(self, az_create_acr_mock):
        context = TestContext()
        context.acr_home = 'existing_home'
        context.acr_user = 'existing_user'
        context.acr_pw = 'existing_pw'
        context.set_input('Setup a new ACR instead (y/N)?', 'y')
        az_create_acr_mock.return_value = 'server', 'user', 'pw'
        server, pw = validate_and_create_acr(context, 'root', 'rg', 'act')
        az_create_acr_mock.assert_called_once_with(context, 'root', 'rg', 'act')
        self.assertEqual(server, 'server')
        self.assertEqual(pw, 'pw')
        self.assertEqual(context.acr_user, 'user')

    @patch('azure.cli.command_modules.ml.env.az_create_acr')
    def test_validate_and_create_acr_set_up_n(self, az_create_acr_mock):
        context = TestContext()
        context.acr_home = 'existing_home'
        context.acr_user = 'existing_user'
        context.acr_pw = 'existing_pw'
        context.set_input('Setup a new ACR instead (y/N)?', '')
        az_create_acr_mock.return_value = 'server', 'user', 'pw'
        server, pw = validate_and_create_acr(context, 'root', 'rg', 'act')
        az_create_acr_mock.assert_not_called()
        self.assertEqual(server, 'existing_home')
        self.assertEqual(pw, 'existing_pw')
        self.assertEqual(context.acr_user, 'existing_user')

    @patch('azure.cli.command_modules.ml.env.az_create_acs')
    def test_validate_and_create_acs_none_set_up(self, az_create_acs_mock):
        context = TestContext()
        context.acr_user = 'acr_user'
        validate_and_create_acs(context, 'root', 'rg', 'acr_login', 'acr_password',
                                'ssh')
        az_create_acs_mock.assert_called_once_with('root', 'rg', 'acr_login', 'acr_user',
                                                   'acr_password', 'ssh')

    @patch('azure.cli.command_modules.ml.env.az_create_acs')
    def test_validate_and_create_acs_set_up_y(self, az_create_acs_mock):
        context = TestContext()
        context.acs_master_url = 'existing_master'
        context.acs_agent_url = 'existing_agent'
        context.acr_user = 'acr_user'
        context.set_input('Setup a new ACS instead (y/N)?', 'y')
        validate_and_create_acs(context, 'root', 'rg', 'acr_login', 'acr_password',
                                'ssh')
        az_create_acs_mock.assert_called_once_with('root', 'rg', 'acr_login', 'acr_user',
                                                   'acr_password', 'ssh')

    @patch('azure.cli.command_modules.ml.env.az_create_acs')
    def test_validate_and_create_acs_set_up_n(self, az_create_acs_mock):
        context = TestContext()
        context.acs_master_url = 'existing_master'
        context.acs_agent_url = 'existing_agent'
        context.acr_user = 'acr_user'
        context.set_input('Setup a new ACS instead (y/N)?', '')
        validate_and_create_acs(context, 'root', 'rg', 'acr_login', 'acr_password',
                                'ssh')
        az_create_acs_mock.assert_not_called()

    @patch('azure.cli.command_modules.ml.env.os.path.exists')
    def test_validate_and_read_ssh_no_ssh_keys_linux_setup_fail(self,
                                                                mock_os_path_exists):
        mock_os_path_exists.return_value = False
        context = TestContext()
        context.set_cmd_result(['ssh-keygen', '-t', 'rsa', '-b', '2048', '-f',
                                self.rsa_private],
                               subprocess.CalledProcessError('an', 'error'))
        context.set_os_name("Linux")

        try:
            validate_and_read_ssh_keys(context)
            self.fail("Should throw.")
        except SshKeygenError:
            pass

        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")

        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Setting up ssh key pair\n"
                                 "Failed to set up ssh key pair. Aborting environment setup.")

    @patch('azure.cli.command_modules.ml.env.os.path.exists')
    def test_validate_and_read_ssh_no_ssh_keys_linux_setup_success_public_key_err(self,
                                                                                  mock_os_path_exists):
        mock_os_path_exists.return_value = False
        context = TestContext()
        context.set_cmd_result(['ssh-keygen', '-t', 'rsa', '-b', '2048', '-f',
                                self.rsa_private], 0)
        context.set_os_name("Linux")
        m = mock_open()
        m.side_effect = IOError

        with patch('azure.cli.command_modules.ml.env.open', m, create=True):
            try:
                validate_and_read_ssh_keys(context)
                self.fail("Should throw.")
            except SshKeygenError:
                pass
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")

        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Setting up ssh key pair\n"
                                 "Could not load your SSH public key from {}\n"
                                 "Please run aml env setup again to create a new ssh keypair.".format(
            self.rsa_public))

    @patch('azure.cli.command_modules.ml.env.os.path.exists')
    def test_validate_and_read_ssh_no_ssh_keys_linux_setup_happy(self,
                                                                 mock_os_path_exists):
        mock_os_path_exists.return_value = False
        context = TestContext()
        context.set_cmd_result(['ssh-keygen', '-t', 'rsa', '-b', '2048', '-f',
                                self.rsa_private], 0)
        context.set_os_name("Linux")
        m = mock_open(read_data='a public key')
        with patch('azure.cli.command_modules.ml.env.open', m, create=True):
            result = validate_and_read_ssh_keys(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")

        self.assertEqual(result, 'a public key')

        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, "Setting up ssh key pair")

    @patch('azure.cli.command_modules.ml.env.os.path.exists')
    def test_validate_and_read_ssh_no_ssh_keys_windows(self, mock_os_path_exists):
        mock_os_path_exists.return_value = False
        context = TestContext()
        context.set_os_name("Windows")
        try:
            validate_and_read_ssh_keys(context)
            self.fail("Should throw.")
        except SshKeygenError:
            pass
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")

        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         "Unable to automatically configure ssh keys on Windows.\n"
                         "Please genereate and place the keys here:\n"
                         "  private key: {}\n"
                         "  public key: {}".format(self.rsa_private, self.rsa_public))

    def test_report_acs_success_linux(self):
        m = mock_open()
        c = TestContext()
        master = 'test.master'
        agent = 'test.agent'

        c.set_os_name('Linux')
        with patch('azure.cli.command_modules.ml.env.open', m, create=True):
            report_acs_success(c, master, agent)

        self.assertEqual(m.mock_calls, [call(self.amlenvrc_path, 'a+'),
                                        call().__enter__(),
                                        call().write(
                                            "export AML_ACS_MASTER=test.master\n"),
                                        call().write(
                                            "export AML_ACS_AGENT=test.agent\n"),
                                        call().__exit__(None, None, None)])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        # self.assertTrue('local' in c.read_config())
        self.assertEqual(output, "ACS deployment succeeded.\n"
                                 "ACS Master URL     : test.master\n"
                                 "ACS Agent URL      : test.agent\n"
                                 "ACS admin username : acsadmin (Needed to set up port forwarding in cluster mode).\n"
                                 "To configure aml with this environment, set the following environment variables.\n"
                                 " export AML_ACS_MASTER=test.master\n"
                                 " export AML_ACS_AGENT=test.agent\n"
                                 "You can also find these settings saved in {}\n"
                                 "\n"
                                 "To switch to cluster mode, run 'aml env cluster'.".format(
            self.amlenvrc_path))

    def test_report_acs_success_windows(self):
        m = mock_open()
        c = TestContext()
        master = 'test.master'
        agent = 'test.agent'
        c.set_os_name('Windows')
        with patch('azure.cli.command_modules.ml.env.open', m, create=True):
            report_acs_success(c, master, agent)
        self.assertEqual(m.mock_calls, [call(self.amlenvrc_path, 'a+'),
                                        call().__enter__(),
                                        call().write(
                                            "set AML_ACS_MASTER=test.master\n"),
                                        call().write(
                                            "set AML_ACS_AGENT=test.agent\n"),
                                        call().__exit__(None, None, None)])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        # self.assertTrue('local' in c.read_config())
        self.assertEqual(output, "ACS deployment succeeded.\n"
                                 "ACS Master URL     : test.master\n"
                                 "ACS Agent URL      : test.agent\n"
                                 "ACS admin username : acsadmin (Needed to set up port forwarding in cluster mode).\n"
                                 "To configure aml with this environment, set the following environment variables.\n"
                                 " set AML_ACS_MASTER=test.master\n"
                                 " set AML_ACS_AGENT=test.agent\n"
                                 "You can also find these settings saved in {}\n"
                                 "\n"
                                 "To switch to cluster mode, run 'aml env cluster'.".format(
            self.amlenvrc_path))

    def test_report_acs_success_linux_IOError(self):
        m = mock_open()
        m.side_effect = IOError
        c = TestContext()
        master = 'test.master'
        agent = 'test.agent'
        c.set_os_name('Linux')
        with patch('azure.cli.command_modules.ml.env.open', m, create=True):
            report_acs_success(c, master, agent)
        self.assertEqual(m.mock_calls, [call(self.amlenvrc_path, 'a+')])
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        # self.assertTrue('local' in c.read_config())
        self.assertEqual(output, "ACS deployment succeeded.\n"
                                 "ACS Master URL     : test.master\n"
                                 "ACS Agent URL      : test.agent\n"
                                 "ACS admin username : acsadmin (Needed to set up port forwarding in cluster mode).\n"
                                 "To configure aml with this environment, set the following environment variables.\n"
                                 " export AML_ACS_MASTER=test.master\n"
                                 " export AML_ACS_AGENT=test.agent\n"
                                 "\n"
                                 "To switch to cluster mode, run 'aml env cluster'.".format(
            self.amlenvrc_path))

    @patch('azure.cli.command_modules.ml.env.env_local')
    def test_env_switching_logic_local(self, env_local_mock):
        c = TestContext()
        c.set_args(['aml', 'env', 'local'])
        env(c)
        env_local_mock.assert_called_once_with(c, [])

    @patch('azure.cli.command_modules.ml.env.env_about')
    def test_env_switching_logic_about(self, env_about_mock):
        c = TestContext()
        c.set_args(['aml', 'env', 'about'])
        env(c)
        env_about_mock.assert_called_once_with()

    @patch('azure.cli.command_modules.ml.env.env_cluster')
    def test_env_switching_logic_cluster(self, env_cluster_mock):
        c = TestContext()
        c.set_args(['aml', 'env', 'cluster'])
        env(c)
        env_cluster_mock.assert_called_once_with(c, [])

    @patch('azure.cli.command_modules.ml.env.env_describe')
    def test_env_switching_logic_show(self, env_describe_mock):
        c = TestContext()
        c.set_args(['aml', 'env', 'show'])
        env(c)
        env_describe_mock.assert_called_once_with(c)

    @patch('azure.cli.command_modules.ml.env.env_setup')
    def test_env_switching_logic_setup(self, env_setup_mock):
        c = TestContext()
        c.set_args(['aml', 'env', 'setup'])
        env(c)
        env_setup_mock.assert_called_once_with(c, [])

    @patch('azure.cli.command_modules.ml.env.env_usage')
    def test_env_switching_logic_unhappy(self, env_usage_mock):
        c = TestContext()
        c.set_args(['aml', 'env', 'trash'])
        env(c)
        env_usage_mock.assert_called_once_with()


if __name__ == '__main__':
    assert not hasattr(sys.stdout, "getvalue")
    unittest.main(module=__name__, buffer=True, exit=False)
