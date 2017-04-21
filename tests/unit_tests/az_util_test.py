import sys
import unittest
import subprocess
import json
from mock import patch
from mocks import TestContext
from mocks import MockProcess
from azure.cli.core.util import CLIError
from azure.cli.command_modules.ml._az_util import validate_env_name
from azure.cli.command_modules.ml._az_util import az_login
from azure.cli.command_modules.ml._az_util import az_check_subscription
from azure.cli.command_modules.ml._az_util import az_create_resource_group
from azure.cli.command_modules.ml._az_util import az_register_provider
from azure.cli.command_modules.ml._az_util import az_create_storage_account
from azure.cli.command_modules.ml._az_util import InvalidNameError
from azure.cli.command_modules.ml._az_util import AzureCliError
from azure.cli.command_modules.ml._az_util import az_create_acr


class AzUtilTest(unittest.TestCase):
    def test_validate_env_name_empty(self):
        try:
            validate_env_name('')
            self.fail('Expected exception for empty name.')
        except InvalidNameError:
            pass

    def test_validate_env_name_too_long(self):
        try:
            validate_env_name('reallyreallyreallyreallylongname')
            self.fail('Expected exception for name too long.')
        except InvalidNameError:
            pass

    def test_validate_env_name_invalid_char(self):
        try:
            validate_env_name('an_invalid_name')
            self.fail('Expected exception for invalid characters.')
        except InvalidNameError:
            pass

    def test_validate_env_name_happy(self):
        validate_env_name('test')

    @patch('azure.cli.core._profile.Profile')
    def test_az_login_show_works(self, profile_mock):
        az_login()
        profile_mock.assert_called_once()

    @patch('azure.cli.core._profile.Profile', autospec=True)
    def test_az_login_run_az_login(self, profile_mock):
        profile_mock().get_subscription.side_effect = CLIError("'az login'")
        az_login()
        profile_mock().get_subscription.assert_called_once()
        profile_mock().find_subscriptions_on_login.assert_called_once()

    @patch('azure.cli.core._profile.Profile', autospec=True)
    def test_az_login_set_account(self, profile_mock):
        profile_mock().get_subscription.side_effect = CLIError("'az account set'")
        try:
            az_login()
            self.fail('expected uncaught error')
        except CLIError:
            pass

        profile_mock().get_subscription.assert_called_once()
        profile_mock().find_subscriptions_on_login.assert_not_called()

    @patch('azure.cli.core._profile.Profile', autospec=True)
    def test_az_login_other_err(self, profile_mock):
        profile_mock().get_subscription.side_effect = CLIError("error")
        try:
            az_login()
            self.fail('expected uncaught error')
        except CLIError:
            pass

        profile_mock().get_subscription.assert_called_once()
        profile_mock().find_subscriptions_on_login.assert_not_called()

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    def test_az_check_subscription_error(self, check_output_mock):
        check_output_mock.side_effect = subprocess.CalledProcessError('', '')
        try:
            az_check_subscription()
            self.fail('Expected exception to be thrown.')
        except AzureCliError:
            pass
        self.assertEqual(check_output_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    def test_az_check_subscription_no_name(self, check_output_mock):
        check_output_mock.return_value = json.dumps({})
        try:
            az_check_subscription()
            self.fail('Expected exception to be thrown.')
        except AzureCliError:
            pass
        self.assertEqual(check_output_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.input')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    def test_az_check_subscription_use_existing(self, check_output_mock, input_mock):
        check_output_mock.return_value = json.dumps({'name': 'test'})
        input_mock.return_value = ''
        az_check_subscription()
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Subscription set to test')
        self.assertEqual(check_output_mock.call_count, 1)
        self.assertEqual(input_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_call')
    @patch('azure.cli.command_modules.ml._az_util.input')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    def test_az_check_subscription_use_another_happy(self, check_output_mock, input_mock,
                                                     check_call_mock):
        check_output_mock.return_value = json.dumps({'name': 'test'})
        input_mock.side_effect = ['no', 'another']
        az_check_subscription()
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Subscription set to test\n'
                                 'Subscription updated to another')
        self.assertEqual(check_output_mock.call_count, 1)
        self.assertEqual(input_mock.call_count, 2)
        self.assertEqual(check_call_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_call')
    @patch('azure.cli.command_modules.ml._az_util.input')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    def test_az_check_subscription_use_another_exc(self, check_output_mock, input_mock,
                                                     check_call_mock):
        check_output_mock.return_value = json.dumps({'name': 'test'})
        input_mock.side_effect = ['no', 'another']
        check_call_mock.side_effect = subprocess.CalledProcessError('', '')
        try:
            az_check_subscription()
            self.fail('Excpected AzureCliError to be thrown.')
        except AzureCliError:
            pass

        self.assertEqual(check_output_mock.call_count, 1)
        self.assertEqual(input_mock.call_count, 2)
        self.assertEqual(check_call_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    def test_az_create_resource_group_no_exist_happy(self, check_output_mock):
        check_output_mock.side_effect = ['false', None]
        rg_name = az_create_resource_group(TestContext(), 'base')
        self.assertEqual(rg_name, 'baserg')
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    def test_az_create_resource_group_exists(self, check_output_mock):
        check_output_mock.side_effect = ['true']
        rg_name = az_create_resource_group(TestContext(), 'base')
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Resource group baserg already exists, skipping creation.')
        self.assertEqual(rg_name, 'baserg')
        self.assertEqual(check_output_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    def test_az_create_resource_group_error(self, check_output_mock):
        check_output_mock.side_effect = [subprocess.CalledProcessError('', ''),
                                         subprocess.CalledProcessError('', '')]
        try:
            az_create_resource_group(TestContext(), 'base')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_call')
    def test_az_register_provider_exc_registering(self, check_call_mock):
        check_call_mock.side_effect = subprocess.CalledProcessError(1, '')
        try:
            az_register_provider('namespace')
            self.fail('Expected exception due to subprocess error.')
        except AzureCliError:
            pass
        check_call_mock.assert_called_once()

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_call')
    def test_az_register_provider_exc_show(self, check_call_mock, check_output_mock):
        check_output_mock.side_effect = subprocess.CalledProcessError(1, '')
        try:
            az_register_provider('namespace')
            self.fail('Expected exception due to subprocess error.')
        except AzureCliError:
            pass
        check_call_mock.assert_called_once()
        check_output_mock.assert_called_once()

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_call')
    def test_az_register_provider_bad_json(self, check_call_mock, check_output_mock):
        check_output_mock.side_effect = 'bad{json'
        try:
            az_register_provider('namespace')
            self.fail('Expected exception due to subprocess error.')
        except AzureCliError:
            pass
        check_call_mock.assert_called_once()
        check_output_mock.assert_called_once()

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_call')
    def test_az_register_provider_empty_json(self, check_call_mock, check_output_mock):
        check_output_mock.return_value = json.dumps({})
        try:
            az_register_provider('namespace')
            self.fail('Expected exception due to subprocess error.')
        except AzureCliError:
            pass
        check_call_mock.assert_called_once()
        check_output_mock.assert_called_once()

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_call')
    def test_az_register_provider_happy(self, check_call_mock, check_output_mock):
        check_output_mock.return_value = json.dumps({'registrationState': 'Registered'})
        self.assertTrue(az_register_provider('namespace'))

        check_call_mock.assert_called_once()
        check_output_mock.assert_called_once()

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_call')
    def test_az_register_provider_happy_loop(self, check_call_mock, check_output_mock):
        check_output_mock.side_effect = [json.dumps({'registrationState': 'Registering..'}),
                                         json.dumps({'registrationState': 'Registered'})]
        self.assertTrue(az_register_provider('namespace'))

        check_call_mock.assert_called_once()
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_exc_already_taken_fail(self, register_provider_mock, check_output_mock):
        check_output_mock.side_effect = [subprocess.CalledProcessError(1, '', 'already taken'), subprocess.CalledProcessError(1, '', 'something else.')]
        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 2)
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_json_exc(self, register_provider_mock, check_output_mock):
        check_output_mock.return_value = 'malformed{json'
        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_empty_json_dict(self, register_provider_mock, check_output_mock):
        check_output_mock.return_value = '{}'
        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_storage_key_fails(self, register_provider_mock, check_output_mock):
        check_output_mock.side_effect = [json.dumps({'provisioningState': 'Succeeded'}),
                                         subprocess.CalledProcessError(1, '', 'some output')]

        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_storage_key_malformed_json(self, register_provider_mock, check_output_mock):
        check_output_mock.side_effect = [json.dumps({'provisioningState': 'Succeeded'}),
                                         'malformed{json']

        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_storage_key_no_key(self, register_provider_mock, check_output_mock):
        check_output_mock.side_effect = [json.dumps({'provisioningState': 'Succeeded'}),
                                         json.dumps({'thing1': 'one',
                                                     'thing2': 'two'})]

        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_storage_key_no_key(self, register_provider_mock, check_output_mock):
        check_output_mock.side_effect = [json.dumps({'provisioningState': 'Succeeded'}),
                                         json.dumps({'thing1': 'one',
                                                     'thing2': 'two'})]

        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_storage_key_one_key(self, register_provider_mock, check_output_mock):
        check_output_mock.side_effect = [json.dumps({'provisioningState': 'Succeeded'}),
                                         json.dumps({'keys': [{}]})]

        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_storage_key_no_key_name(self, register_provider_mock, check_output_mock):
        check_output_mock.side_effect = [json.dumps({'provisioningState': 'Succeeded'}),
                                         json.dumps({'keys': [{}, {}]})]

        try:
            az_create_storage_account(TestContext(), 'root', 'rg')
            self.fail('Expected exception to be raised.')
        except AzureCliError:
            pass
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_storage_account_storage_key_happy(self, register_provider_mock, check_output_mock):
        check_output_mock.side_effect = [json.dumps({'provisioningState': 'Succeeded'}),
                                         json.dumps({'keys': [{}, {'keyName': 'primary',
                                                                   'value': 'test_value'}]})]

        storage_name, key_value = az_create_storage_account(TestContext(), 'root', 'rg')
        self.assertEqual(storage_name, 'rootstor')
        self.assertEqual(key_value, 'test_value')
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.Popen')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_acr_in_use_error(self, register_provider_mock, popen_mock):
        popen_mock.side_effect = [MockProcess('', 'already in use'),
                                  subprocess.CalledProcessError(1, '', '')]
        try:
            az_create_acr(TestContext(), 'root', 'rg', 'rootstor')
            self.fail('Expected exception.')
        except AzureCliError:
            pass
        self.assertEqual(popen_mock.call_count, 2)
        self.assertEqual(register_provider_mock.call_count, 2)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.Popen')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_acr_malformed_json(self, register_provider_mock, popen_mock):
        popen_mock.side_effect = [MockProcess('', 'malformed}json')]
        try:
            az_create_acr(TestContext(), 'root', 'rg', 'rootstor')
            self.fail('Expected exception.')
        except AzureCliError:
            pass
        self.assertEqual(popen_mock.call_count, 1)
        self.assertEqual(register_provider_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.Popen')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_acr_empty_json(self, register_provider_mock, popen_mock):
        popen_mock.side_effect = [MockProcess('{}', 'Some erro')]
        try:
            az_create_acr(TestContext(), 'root', 'rg', 'rootstor')
            self.fail('Expected exception.')
        except AzureCliError:
            pass
        self.assertEqual(popen_mock.call_count, 1)
        self.assertEqual(register_provider_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.Popen')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_acr_happy_start_cred_fail(self, register_provider_mock, popen_mock, check_output_mock):
        popen_mock.side_effect = [MockProcess(json.dumps({'loginServer': 'login'}), 'Some erro')]
        check_output_mock.side_effect = subprocess.CalledProcessError(1, '2', '3')
        try:
            az_create_acr(TestContext(), 'root', 'rg', 'rootstor')
            self.fail('Expected exception.')
        except AzureCliError:
            pass
        self.assertEqual(popen_mock.call_count, 1)
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.Popen')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_acr_happy_start_cred_malformed_json(self, register_provider_mock, popen_mock, check_output_mock):
        popen_mock.side_effect = [MockProcess(json.dumps({'loginServer': 'login'}), 'Some erro')]
        check_output_mock.side_effect = 'malformed{json'
        try:
            az_create_acr(TestContext(), 'root', 'rg', 'rootstor')
            self.fail('Expected exception.')
        except AzureCliError:
            pass
        self.assertEqual(popen_mock.call_count, 1)
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 1)

    @patch('azure.cli.command_modules.ml._az_util.subprocess.check_output')
    @patch('azure.cli.command_modules.ml._az_util.subprocess.Popen')
    @patch('azure.cli.command_modules.ml._az_util.az_register_provider')
    def test_az_create_acr_happy_start_cred_malformed_json(self, register_provider_mock, popen_mock, check_output_mock):
        popen_mock.side_effect = [MockProcess(json.dumps({'loginServer': 'login'}), 'Some erro')]
        check_output_mock.side_effect = 'malformed{json'
        try:
            az_create_acr(TestContext(), 'root', 'rg', 'rootstor')
            self.fail('Expected exception.')
        except AzureCliError:
            pass
        self.assertEqual(popen_mock.call_count, 1)
        self.assertEqual(register_provider_mock.call_count, 1)
        self.assertEqual(check_output_mock.call_count, 1)


if __name__ == "__main__":
    assert not hasattr(sys.stdout, "getvalue")
    unittest.main(module=__name__, buffer=True, exit=False)
