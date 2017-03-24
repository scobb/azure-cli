import unittest
import json
import sys
import os
from mock import patch
import datetime
import azure.cli.command_modules.ml.service._util as cli_util
from .mocks import TestContext
from .mocks import CorruptConfigTestContext
from .mocks import MockResponse
from .mocks import MockHttpResponse


class CliUtilUnitTests(unittest.TestCase):
    """
    Unit tests for Batch CLI
    """

    test_location = os.path.split(os.path.realpath(__file__))[0]
    relative_path_to_resources = os.path.join('..', 'test_resources')
    path_to_resources = os.path.join(test_location, relative_path_to_resources)
    path_to_empty_resource = os.path.join(test_location, 'empty_resource')

    def test_cli_context_constructor(self):
        cli_util.CommandLineInterfaceContext.hdi_home = 'https://azuremlbatchint-aml.apps.azurehdinsight.net'
        context = cli_util.CommandLineInterfaceContext()
        self.assertEqual(context.hdi_home, 'azuremlbatchint-aml.apps.azurehdinsight.net')
        self.assertEqual(context.hdi_domain, 'azuremlbatchint-aml')

    def test_str_from_subprocess_communicate_str(self):
        # this test is redundant in python2, but provides extra coverage for python3
        output = 'some_output'
        result = cli_util.CommandLineInterfaceContext.str_from_subprocess_communicate(output)
        self.assertEqual(output, result)

    def test_str_from_subprocess_communicate_bytes(self):
        expected = 'some_output'
        result = cli_util.CommandLineInterfaceContext.str_from_subprocess_communicate(expected.encode('utf-8'))
        self.assertEqual(expected, result)

    @staticmethod
    def get_next_version_check_time():
        return (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    def test_get_json_happy_str(self):
        json_obj = {"oh": "hi"}
        json_str = json.dumps(json_obj)
        obj_again = cli_util.get_json(json_str)
        self.assertEqual(json_obj, obj_again)

    def test_get_json_happy_bytes(self):
        json_obj = {"oh": "hi"}
        json_str = str.encode(json.dumps(json_obj))
        obj_again = cli_util.get_json(json_str)
        self.assertEqual(json_obj, obj_again)

    def test_get_json_unhappy(self):
        empty_obj = cli_util.get_json(None)
        self.assertEqual(empty_obj, {})

    def test_get_json_malformed(self):
        try:
            cli_util.get_json('{')
            self.fail('Expected value error from malformed string.')
        except ValueError:
            pass

    def test_check_version_up_to_date(self):
        context = TestContext()
        context.set_cmd_result('pip search azuremlcli', ('''azuremlcli (0.2rc278)  - Microsoft Azure Machine Learning Command Line Tools
  INSTALLED: 0.2rc230
  LATEST:    0.2rc278
''', ''))
        cli_util.check_version(context, context.read_config())
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        expected_output = '\x1b[93mYou are using AzureML CLI version 0.2rc230, but ' \
                          'version 0.2rc278 is available.\nYou should consider ' \
                          'upgrading via the \'pip install --upgrade azuremlcli\' ' \
                          'command.\x1b[0m'
        self.assertEqual(output, expected_output)
        self.assertEquals(context.read_config()['next_version_check'],
                          self.get_next_version_check_time())

    def test_check_version_not_up_to_date(self):
        context = TestContext()
        context.set_cmd_result('pip search azuremlcli', ('''azuremlcli (0.2rc278)  - Microsoft Azure Machine Learning Command Line Tools
  INSTALLED: 0.2rc278 (latest)
''', ''))
        cli_util.check_version(context, context.read_config())
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEquals(output, '')
        self.assertEquals(context.read_config()['next_version_check'],
                          self.get_next_version_check_time())

    def test_check_version_exception(self):
        context = TestContext()
        context.set_cmd_result('pip search azuremlcli', Exception())
        # context.set_cmd_result('pip list -o --pre', Exception())
        cli_util.check_version(context, context.read_config())
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEquals(output, 'Warning: Error determining if there is a newer version of AzureML CLI available:')

    @patch('azure.cli.command_modules.ml.service._util.check_version')
    def test_first_run_no_version_check(self, check_version_mock):
        context = TestContext()
        context.set_cmd_result('pip list -o --pre', ('', ''))
        config = {'next_version_check': self.get_next_version_check_time()}
        context.write_config(config)
        cli_util.first_run(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEquals(output, '')
        # should not call pip list
        check_version_mock.assert_not_called()

        # should not update the config
        self.assertEqual(context.read_config(), config)

    @patch('azure.cli.command_modules.ml.service._util.check_version')
    def test_first_run_no_config_version_check(self, check_version_mock):
        context = TestContext()
        context.write_config({})
        cli_util.first_run(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEquals(output, '')

        # should call pip list exactly once
        check_version_mock.assert_called_once()

        # should set to local mode
        self.assertEquals(context.read_config(),
                          {'mode': 'local'})

    @patch('azure.cli.command_modules.ml.service._util.check_version')
    def test_first_run_valid_config_version_check(self, check_version_mock):
        context = TestContext()
        context.write_config({'mode': 'cluster'})
        cli_util.first_run(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEquals(output, '')

        # should call pip list exactly once
        check_version_mock.assert_called_once()

        # should only update the next_version_check field
        self.assertEquals(context.read_config(),
                          {'mode': 'cluster'})

    @patch('azure.cli.command_modules.ml.service._util.check_version')
    def test_first_run_corrupt_config_version_check(self, check_version_mock):
        context = CorruptConfigTestContext()
        cli_util.first_run(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEquals(output, 'Warning: Azure ML configuration file is corrupt.')

        # should call pip list exactly once
        check_version_mock.assert_called_once()

        # should only update the next_version_check field
        self.assertEquals(context.read_config(),
                          {'mode': 'local'})

    @patch('azure.cli.command_modules.ml.service._util.check_version')
    def test_first_run_corrupt_next_version_check(self, check_version_mock):
        context = TestContext()
        context.write_config({'next_version_check': 'some_trash_value'})
        cli_util.first_run(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEquals(output, 'Warning: Azure ML configuration file is corrupt.')

        # should call pip list exactly once
        check_version_mock.assert_called_once()

        # should set to local mode
        self.assertEquals(context.read_config()['mode'], 'local')

    def test_process_errors_happy_string(self):
        content = json.dumps({'error': {'details': [{'message': 'An error message.'}]}})
        http_response = MockHttpResponse(content, 400)
        to_print = cli_util.process_errors(http_response)
        self.assertEqual(to_print, 'Failed.\nResponse code: 400\nAn error message.')

    def test_process_errors_happy_byte(self):
        content = json.dumps({'error': {'details': [{'message': 'An error message.'}]}})\
            .encode('utf-8')
        self.assertTrue(isinstance(content, bytes))
        http_response = MockHttpResponse(content, 400)
        to_print = cli_util.process_errors(http_response)
        self.assertEqual(to_print, 'Failed.\nResponse code: 400\nAn error message.')

    def test_process_errors_unhappy(self):
        content = 'an unformatted error'
        http_response = MockHttpResponse(content, 400)
        to_print = cli_util.process_errors(http_response)
        self.assertEqual(to_print, 'Failed.\nResponse code: 400\nan unformatted error')

    def test_process_errors_unhappy_json(self):
        content = json.dumps({'error': {'detail': [{'message': 'An error message.'}]}})
        http_response = MockHttpResponse(content, 400)
        to_print = cli_util.process_errors(http_response)
        self.assertEqual(to_print, 'Failed.\nResponse code: 400\n{"error": {"detail": [{"message": "An error message."}]}}')

    def test_get_success_and_resp_str_unhappy_None(self):
        context = TestContext()
        succeeded, to_print = cli_util.get_success_and_resp_str(context, None)
        self.assertFalse(succeeded)
        self.assertEquals(to_print, "Response was None.")

    def test_get_success_and_resp_str_unhappy_unformatted_verbose(self):
        context = TestContext()
        http_response = MockHttpResponse('an unformatted error', 400)
        succeeded, to_print = cli_util.get_success_and_resp_str(context, http_response, verbose=True)
        self.assertFalse(succeeded)
        self.assertEquals(to_print, "Failed.\nResponse code: 400\nan unformatted error")
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'an unformatted error')

    def test_get_success_and_resp_str_unformatted_success_no_handler(self):
        context = TestContext()
        http_response = MockHttpResponse('an unformatted success', 200)
        succeeded, to_print = cli_util.get_success_and_resp_str(context, http_response)
        self.assertTrue(succeeded)
        self.assertEquals(to_print, "an unformatted success")

    def test_get_success_and_resp_str_formatted_success_no_handler(self):
        context = TestContext()
        content = json.dumps({'job_id': 'some_numbers_here'})
        http_response = MockHttpResponse(content, 200)
        succeeded, to_print = cli_util.get_success_and_resp_str(context, http_response)
        expected_response = '{\n    "job_id": "some_numbers_here"\n}'
        self.assertTrue(succeeded)
        self.assertEquals(to_print, expected_response)

    def test_get_success_and_resp_str_unformatted_success_with_handler(self):
        context = TestContext()
        http_response = MockHttpResponse('an unformatted success', 200)
        handler = MockResponse()
        succeeded, to_print = cli_util.get_success_and_resp_str(context, http_response, response_obj=handler)
        self.assertTrue(succeeded)
        self.assertEquals(to_print, "an unformatted success")

    def test_get_success_and_resp_str_formatted_success_with_handler(self):
        context = TestContext()
        content = json.dumps({'job_id': 'some_numbers_here'})
        http_response = MockHttpResponse(content, 200)
        handler = MockResponse()
        succeeded, to_print = cli_util.get_success_and_resp_str(context, http_response, response_obj=handler)
        expected_response = '{"job_id": "some_numbers_here"}'
        self.assertTrue(succeeded)
        self.assertEquals(to_print, expected_response)

    def test_validate_remote_filepath_local(self):
        context = TestContext()
        context.set_local_mode(True)
        try:
            cli_util.validate_remote_filepath(context, 'any input here.')
            self.fail('remote filepaths should fail in local mode')
        except ValueError:
            pass

    def test_validate_remote_filepath_default_wasb_happy(self):
        context = TestContext()
        context.set_local_mode(False)
        try:
            cli_util.validate_remote_filepath(context, 'wasb:///any/path/here.csv')
        except ValueError:
            self.fail('Should not have thrown for default wasb addresses')

    def test_validate_remote_filepath_default_wasbs_happy(self):
        context = TestContext()
        context.set_local_mode(False)
        try:
            cli_util.validate_remote_filepath(context, 'wasbs:///any/path/here.csv')
        except ValueError:
            self.fail('Should not have thrown for default wasbs addresses')

    def test_validate_remote_filepath_wasb_happy(self):
        context = TestContext()
        context.set_local_mode(False)
        context.az_account_name = 'mystorage'
        try:
            cli_util.validate_remote_filepath(context,
                                              'wasb://container@mystorage.blob.core.windows.net/any/path/here.csv')
        except ValueError:
            self.fail('Should not have thrown for valid wasb addresses')

    def test_validate_remote_filepath_wasbs_happy(self):
        context = TestContext()
        context.set_local_mode(False)
        context.az_account_name = 'mystorage'
        try:
            cli_util.validate_remote_filepath(context, 'wasbs://container@mystorage.blob.core.windows.net/any/path/here.csv')
        except ValueError:
            self.fail('Should not have thrown for valid wasbs addresses')

    def test_validate_remote_filepath_http_happy(self):
        context = TestContext()
        context.set_local_mode(False)
        context.az_account_name = 'mystorage'
        try:
            cli_util.validate_remote_filepath(context,
                                              'http://mystorage.blob.core.windows.net/any/path/here.csv')
        except ValueError:
            self.fail('Should not have thrown for valid http addresses')

    def test_validate_remote_filepath_https_happy(self):
        context = TestContext()
        context.set_local_mode(False)
        context.az_account_name = 'mystorage'
        try:
            cli_util.validate_remote_filepath(context,
                                              'https://mystorage.blob.core.windows.net/any/path/here.csv')
        except ValueError:
            self.fail('Should not have thrown for valid https addresses')

    def test_validate_remote_filepath_wasb_unhappy(self):
        context = TestContext()
        context.set_local_mode(False)
        context.az_account_name = 'anotherstorage'
        try:
            cli_util.validate_remote_filepath(context,
                                              'wasb://container@mystorage.blob.core.windows.net/any/path/here.csv')
            self.fail('Should have thrown for invalid wasb addresses')
        except ValueError:
            pass

    def test_validate_remote_filepath_http_unhappy(self):
        context = TestContext()
        context.set_local_mode(False)
        context.az_account_name = 'anotherstorage'
        try:
            cli_util.validate_remote_filepath(context, 'http://mystorage.blob.core.windows.net/any/path/here.csv')
            self.fail('Should have thrown for invalid http addresses')
        except ValueError:
            pass

    def test_update_local_asset_path_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        resource = os.path.join(CliUtilUnitTests.path_to_resources, '0.txt')
        asset_id, location = cli_util.update_asset_path(context, True, resource, 'a_container')
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(output.startswith('filepath') and 'uploaded to' in output)
        self.assertTrue(asset_id in context.get_uploaded_resources())
        self.assertEqual(context.get_uploaded_resources()[asset_id], location)

    def test_update_local_asset_path_local(self):
        context = TestContext()
        context.set_local_mode(True)
        resource = os.path.join(CliUtilUnitTests.path_to_resources, '0.txt')
        asset_id, location = cli_util.update_asset_path(context, False, resource, 'a_container')
        self.assertTrue(asset_id in context.get_cached_resources())
        self.assertEqual(context.get_cached_resources()[asset_id], location)

    def test_update_local_directory_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        asset_id, location = cli_util.update_asset_path(context, False,
                                                        CliUtilUnitTests.path_to_resources,
                                                        'a_container')

        # location we get back should be a directory
        self.assertTrue(location.endswith(os.path.basename(
            CliUtilUnitTests.path_to_resources)))

        # we should have uploaded three
        expected_resources = {'0.txt': 'wasb://azureml@mywasbstorage.blob.core.windows.net/a_container/test_resources/0.txt',
                              '1.txt': 'wasb://azureml@mywasbstorage.blob.core.windows.net/a_container/test_resources/1.txt',
                              '2.txt': 'wasb://azureml@mywasbstorage.blob.core.windows.net/a_container/test_resources/subdir/2.txt'
                              }
        self.assertEqual(context.get_uploaded_resources(), expected_resources)

    def test_update_local_directory_local(self):
        context = TestContext()
        context.set_local_mode(True)
        asset_id, location = cli_util.update_asset_path(context, False,
                                                        CliUtilUnitTests.path_to_resources,
                                                        'a_container')

        # directory should be a straight copy here
        self.assertTrue(asset_id in context.get_cached_resources())
        self.assertEqual(context.get_cached_resources()[asset_id], location)

    def test_update_http_asset_local(self):
        context = TestContext()
        context.set_local_mode(True)
        try:
            cli_util.update_asset_path(context, False, 'http://some/resource.txt',
                                       'a_container')
            self.fail('Should have thrown for http asset in local mode.')
        except ValueError:
            pass

    def test_update_wasb_asset_local(self):
        context = TestContext()
        context.set_local_mode(True)
        try:
            cli_util.update_asset_path(context, False,
                                       'wasb://container@some/resource.txt',
                                       'a_container')
            self.fail('Should have thrown for http asset in local mode.')
        except ValueError:
            pass

    def test_update_http_asset_valid_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        context.az_account_name = 'mystorage'
        http_resource = 'http://mystorage.blob.core.windows.net/any/path/here.csv'
        asset_id, location = cli_util.update_asset_path(context, False, http_resource,
                                                        'a_container')
        self.assertEqual(location, http_resource)
        self.assertEqual(asset_id, 'here.csv')

        # no uploads should occur
        self.assertEqual(len(context.get_uploaded_resources()), 0)

    def test_update_wasb_asset_valid_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        context.az_account_name = 'mystorage'
        wasb_resource = 'wasb://container@mystorage.blob.core.windows.net/any/path/here.csv'
        asset_id, location = cli_util.update_asset_path(context, False, wasb_resource,
                                                        'a_container')
        self.assertEqual(location, wasb_resource)
        self.assertEqual(asset_id, 'here.csv')

        # no uploads should occur
        self.assertEqual(len(context.get_uploaded_resources()), 0)

    def test_update_local_input_does_not_exist_local(self):
        context = TestContext()
        context.set_local_mode(True)
        try:
            cli_util.update_asset_path(context, False, 'does_not_exist.txt', 'a_container')
            self.fail('Should have thrown for non-existent local resource.')
        except ValueError:
            pass

    def test_update_local_input_does_not_exist_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        try:
            cli_util.update_asset_path(context, False, 'does_not_exist.txt', 'a_container')
            self.fail('Should have thrown for non-existent local resource.')
        except ValueError:
            pass

    def test_update_local_output_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        try:
            cli_util.update_asset_path(context, False, 'output.csv', 'a_container',
                                       is_input=False)
            self.fail('Should have thrown for local output in cluster mode.')
        except ValueError:
            pass

    def test_upload_directory_empty(self):
        context = TestContext()
        context.set_local_mode(False)
        if not os.path.exists(CliUtilUnitTests.path_to_empty_resource):
            os.makedirs(CliUtilUnitTests.path_to_empty_resource)
        try:
            cli_util.upload_directory(context, CliUtilUnitTests.path_to_empty_resource,
                                      'a_container', False)
            self.fail('Should have thrown for empty directory.')
        except ValueError:
            pass

    def test_upload_directory(self):
        context = TestContext()
        context.set_local_mode(False)
        asset_id, location = cli_util.upload_directory(context, CliUtilUnitTests.path_to_resources,
                                                       'a_container', False)
        expected_resources = {'0.txt': 'wasb://azureml@mywasbstorage.blob.core.windows.net/a_container/test_resources/0.txt',
                              '1.txt': 'wasb://azureml@mywasbstorage.blob.core.windows.net/a_container/test_resources/1.txt',
                              '2.txt': 'wasb://azureml@mywasbstorage.blob.core.windows.net/a_container/test_resources/subdir/2.txt'
                              }
        expected_location = 'wasb://azureml@mywasbstorage.blob.core.windows.net/a_container/test_resources'
        self.assertEqual(location, expected_location)
        self.assertEqual(context.get_uploaded_resources(), expected_resources)

    def test_traverse_json(self):
        json_obj = {'one': {'two': {'three': 'Found it'}}}
        traversal_tuple = ('one', 'two', 'three')
        result = cli_util.traverse_json(json_obj, traversal_tuple)
        self.assertEqual(result, 'Found it')

    def test_traverse_json_unhappy(self):
        json_obj = {'one': {'two': {'three': 'Found it'}}}
        traversal_tuple = ('one', 'two', 'five')
        try:
            cli_util.traverse_json(json_obj, traversal_tuple)
            self.fail('Should throw if traversal tuple does not match json schema.')
        except KeyError:
            pass

    def test_traverse_json_unhappy_list(self):
        json_obj = {'one': {'two': [{'three': 'Found it'}]}}
        traversal_tuple = ('one', 'two', 'three')
        try:
            cli_util.traverse_json(json_obj, traversal_tuple)
            self.fail('Should throw if trying to traverse lists with string keys.')
        except TypeError:
            pass

    def test_traverse_json_happy_list(self):
        json_obj = {'one': {'two': [{'three': 'Found it'}]}}
        traversal_tuple = ('one', 'two', 0, 'three')
        result = cli_util.traverse_json(json_obj, traversal_tuple)
        self.assertEqual(result, 'Found it')

    def test_static_string_response(self):
        context = TestContext()
        response = cli_util.StaticStringResponse('blah')
        resp_str = response.format_successful_response(context, {})
        self.assertEqual('blah', resp_str)

    def test_conditional_list_traversal_function(self):
        context = TestContext()
        tup = ('one', 'two')
        condition = lambda obj: 'foo' in obj and obj['foo']
        action = lambda obj: obj['val']
        json_obj = {'one': {'two': [{'foo': True, 'val': 'print'},
                                    {'val': 'noprint'},
                                    {'foo': False, 'val': 'noprint'},
                                    {'foo': 'evaluates_to_true', 'val': 'print'}]}}
        fn = cli_util.ConditionalListTraversalFunction(tup, condition, action)
        fn.set_json(json_obj)
        to_check = fn.evaluate(context)
        self.assertEqual(to_check, 'print, print')

    def test_is_int(self):
        self.assertTrue(cli_util.is_int('5'))
        self.assertFalse(cli_util.is_int('stuff'))

    def tearDown(self):
        if os.path.exists(CliUtilUnitTests.path_to_empty_resource):
            os.rmdir(CliUtilUnitTests.path_to_empty_resource)


if __name__ == '__main__':
    assert not hasattr(sys.stdout, "getvalue")
    unittest.main(module=__name__, buffer=True, exit=False)
