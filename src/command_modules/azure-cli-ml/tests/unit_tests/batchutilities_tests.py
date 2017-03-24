import unittest
import json
import requests
import sys

from azuremlcli.tests.mocks import TestContext
from azuremlcli.tests.mocks import MockHttpResponse
import azuremlcli.batchutilities as bu


class BatchUtilitiesTests(unittest.TestCase):
    def test_batch_get_url_local(self):
        context = TestContext()
        context.set_local_mode(True)
        url = bu.batch_get_url(context, '{}/{{}}/{{}}', 'some', 'path')
        self.assertEqual(url, 'http://localhost:8080/some/path')

    def test_batch_get_url_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'cluster'
        url = bu.batch_get_url(context, '{}/{{}}/{{}}', 'some', 'path')
        self.assertEqual(url, 'https://cluster-aml.apps.azurehdinsight.net/some/path')

    def test_batch_get_asset_type_py(self):
        asset_id = 'driver.py'
        self.assertEqual(bu.batch_get_asset_type(asset_id), bu.BATCH_PYTHON_ASSET)

    def test_batch_get_asset_type_jar(self):
        asset_id = 'library.jar'
        self.assertEqual(bu.batch_get_asset_type(asset_id), bu.BATCH_JAR_ASSET)

    def test_batch_get_asset_type_file(self):
        asset_id = 'file.csv'
        self.assertEqual(bu.batch_get_asset_type(asset_id), bu.BATCH_FILE_ASSET)

    def test_batch_get_parameter_str(self):
        to_test = [({'Direction': 'Output', 'Id': '--myoutput', 'Kind': 'Reference'},
                    '-o --myoutput=<value>'),
                   (
                       {'Direction': 'Output', 'Id': '--mydefaultoutput',
                        'Kind': 'Reference',
                        'Value': 'MyDefaultValue'}, '[-o --mydefaultoutput=<value>]'),
                   ({'Direction': 'Input', 'Id': '--myinput', 'Kind': 'Reference'},
                    '-i --myinput=<value>'),
                   ({'Direction': 'Input', 'Id': '--mydefaultinput', 'Kind': 'Reference',
                     'Value': 'MyDefaultValue'}, '[-i --mydefaultinput=<value>]'),
                   ({'Direction': 'Input', 'Id': '--myparam', 'Kind': 'Value'},
                    '-p --myparam=<value>'),
                   ({'Direction': 'Input', 'Id': '--mydefaultparam', 'Kind': 'Value',
                     'Value': 'MyDefaultValue'}, '[-p --mydefaultparam=<value>]')]
        for param_dict, expected_str in to_test:
            self.assertEqual(expected_str, bu.batch_get_parameter_str(param_dict))

    def test_batch_get_job_description_remote(self):
        context = TestContext()
        context.hdi_domain = 'testdomain'
        job_payload = {'WebServiceId': 'an_id',
                       'JobId': 'another_id',
                       'YarnAppId': 'yarn_id',
                       'State': 'going!'}
        result = bu.batch_get_job_description(context, json.dumps(job_payload))
        self.assertEqual(result, 'Name: an_id\nJobId: another_id\nYarnAppId: yarn_id\n'
                                 'Logs available at: https://testdomain.azurehdinsight.net/'
                                 'yarnui/hn/cluster/app/yarn_id\nState: going!')

    def test_batch_get_job_description_local(self):
        context = TestContext()
        job_payload = {'WebServiceId': 'an_id',
                       'JobId': 'another_id',
                       'DriverLogFile': 'log_loc',
                       'State': 'going!'}
        result = bu.batch_get_job_description(context, json.dumps(job_payload))
        self.assertEqual(result, 'Name: an_id\nJobId: another_id\nLogs available at: '
                                 'log_loc\nState: going!')

    def test_batch_create_parameter_default(self):
        name = 'a_name'
        value = 'a_value'
        kind = 'Reference'
        direction = 'Input'
        expected = {"Id": name,
                    "IsRuntime": True,
                    "IsOptional": False,
                    "Kind": kind,
                    "Direction": direction,
                    'Value': value}
        actual = bu.batch_create_parameter_entry('{}={}'.format(name, value), kind,
                                                 direction)
        self.assertEqual(actual, expected)

    def test_batch_create_parameter_no_default(self):
        name = 'a_name'
        kind = 'Reference'
        direction = 'Input'
        expected = {"Id": name,
                    "IsRuntime": True,
                    "IsOptional": False,
                    "Kind": kind,
                    "Direction": direction}
        actual = bu.batch_create_parameter_entry(name, kind, direction)
        self.assertEqual(actual, expected)

    def test_batch_create_parameter_list(self):
        to_test = [('--myoutput', 'Output', 'Reference'),
                   ('--myinput', 'Input', 'Reference'),
                   ('--myparam', 'Input', 'Value'),
                   ('--myoutputdefault=default_value', 'Output', 'Reference'),
                   ('--myinputdefault=default_value', 'Input', 'Reference'),
                   ('--myparamdefault=default_value', 'Input', 'Value')]
        actual = bu.batch_create_parameter_list(to_test)
        expected = [{'Kind': 'Reference', 'Direction': 'Output', 'Id': '--myoutput',
                     'IsOptional': False, 'IsRuntime': True},
                    {'Kind': 'Reference', 'Direction': 'Input', 'Id': '--myinput',
                     'IsOptional': False, 'IsRuntime': True},
                    {'Kind': 'Value', 'Direction': 'Input', 'Id': '--myparam',
                     'IsOptional': False, 'IsRuntime': True},
                    {'Kind': 'Reference', 'IsOptional': False, 'Direction': 'Output',
                     'Value': 'default_value', 'IsRuntime': True,
                     'Id': '--myoutputdefault'},
                    {'Kind': 'Reference', 'IsOptional': False, 'Direction': 'Input',
                     'Value': 'default_value', 'IsRuntime': True,
                     'Id': '--myinputdefault'},
                    {'Kind': 'Value', 'IsOptional': False, 'Direction': 'Input',
                     'Value': 'default_value', 'IsRuntime': True,
                     'Id': '--myparamdefault'}]
        self.assertEqual(actual, expected)

    def test_batch_app_is_installed_local(self):
        context = TestContext()
        context.set_local_mode(True)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT), MockHttpResponse('', 200))
        self.assertEqual(bu.batch_app_is_installed(context), 200)

    def test_batch_app_is_installed_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 200))
        self.assertEqual(bu.batch_app_is_installed(context), 200)

    def test_batch_app_503_local(self):
        context = TestContext()
        context.set_local_mode(True)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT), MockHttpResponse('', 503))
        self.assertEqual(bu.batch_app_is_installed(context), 503)

    def test_batch_app_503_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 503))
        self.assertEqual(bu.batch_app_is_installed(context), 503)

    def test_batch_app_exception_local(self):
        context = TestContext()
        context.set_local_mode(True)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           requests.exceptions.ConnectionError())
        self.assertEqual(bu.batch_app_is_installed(context), None)

    def test_batch_app_exception_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           requests.exceptions.ConnectionError())
        self.assertEqual(bu.batch_app_is_installed(context), None)

    def test_batch_get_acceptable_storage_happy_remote(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        resp_json = {'Storage': [{'Value': 'some_storage'}, {'Value': 'some_more_storage'}]}
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse(json.dumps(resp_json), 200))
        acceptable_storage = bu.batch_get_acceptable_storage(context)
        self.assertEqual(acceptable_storage, ['some_storage', 'some_more_storage'])

    def test_batch_get_acceptable_storage_happy_local(self):
        context = TestContext()
        context.set_local_mode(True)
        context.hdi_domain = 'testdomain'
        resp_json = {'Storage': [{'Value': 'some_storage'}, {'Value': 'some_more_storage'}]}
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse(json.dumps(resp_json), 200))
        acceptable_storage = bu.batch_get_acceptable_storage(context)
        self.assertEqual(acceptable_storage, ['some_storage', 'some_more_storage'])

    def test_batch_get_acceptable_storage_exc_remote(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           requests.exceptions.ConnectionError())
        try:
            bu.batch_get_acceptable_storage(context)
            self.fail('Expected InvalidStorageException')
        except bu.InvalidStorageException:
            pass

    def test_batch_get_acceptable_storage_exc_local(self):
        context = TestContext()
        context.set_local_mode(True)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           requests.exceptions.ConnectionError())
        try:
            bu.batch_get_acceptable_storage(context)
            self.fail('Expected InvalidStorageException')
        except bu.InvalidStorageException:
            pass

    def test_batch_get_acceptable_storage_503_remote(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse('Service Unavailable', 503))
        try:
            bu.batch_get_acceptable_storage(context)
            self.fail('Expected InvalidStorageException')
        except bu.InvalidStorageException:
            pass

    def test_batch_get_acceptable_storage_503_local(self):
        context = TestContext()
        context.set_local_mode(True)
        context.hdi_domain = 'testdomain'
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse('Service Unavailable', 503))
        try:
            bu.batch_get_acceptable_storage(context)
            self.fail('Expected InvalidStorageException')
        except bu.InvalidStorageException:
            pass

    def test_batch_get_acceptable_storage_bad_contract_remote(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        resp_json = {'NotStorage': [{'Value': 'some_storage'}, {'Value': 'some_more_storage'}]}
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse(json.dumps(resp_json), 200))
        try:
            bu.batch_get_acceptable_storage(context)
            self.fail('Expected InvalidStorageException')
        except bu.InvalidStorageException:
            pass

    def test_batch_get_acceptable_storage_bad_contract_local(self):
        context = TestContext()
        context.set_local_mode(True)
        context.hdi_domain = 'testdomain'
        resp_json = {'NotStorage': [{'Value': 'some_storage'}, {'Value': 'some_more_storage'}]}
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse(json.dumps(resp_json), 200))
        try:
            bu.batch_get_acceptable_storage(context)
            self.fail('Expected InvalidStorageException')
        except bu.InvalidStorageException:
            pass

    def test_batch_env_is_valid_remote_env_unset(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = None
        context.hdi_user = None
        context.hdi_pw = None
        self.assertFalse(bu.batch_env_is_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Environment is missing the following variables:\n  AML_HDI_CLUSTER\n  '
                                 'AML_HDI_USER\n  AML_HDI_PW\nFor help setting up environment, run\n  aml env about')

    def test_batch_env_is_valid_local_happy(self):
        context = TestContext()
        context.set_local_mode(True)
        # env should not matter here
        context.hdi_domain = None
        context.hdi_user = None
        context.hdi_pw = None
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 200))
        self.assertTrue(bu.batch_env_is_valid(context))

    def test_batch_env_is_valid_local_404(self):
        context = TestContext()
        context.set_local_mode(True)
        # env should not matter here
        context.hdi_domain = None
        context.hdi_user = None
        context.hdi_pw = None
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 404))
        self.assertFalse(bu.batch_env_is_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'AML Batch is not currently installed on http://localhost:8080. '
                                 'Please install the app.')

    def test_batch_env_is_valid_remote_404(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 404))
        self.assertFalse(bu.batch_env_is_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'AML Batch is not currently installed on https://testdomain-aml.apps.'
                                 'azurehdinsight.net. Please install the app.')

    def test_batch_env_is_valid_remote_403(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 403))
        self.assertFalse(bu.batch_env_is_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Authentication failed on https://testdomain-aml.apps.azurehdinsight.net. '
                                 'Check your AML_HDI_USER and AML_HDI_PW environment variables.\n'
                                 'For help setting up environment, run\n  aml env about')

    def test_batch_env_is_valid_remote_503(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 503))
        self.assertFalse(bu.batch_env_is_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Unexpected return code 503 when querying AzureBatch at https://testdomain-aml.apps.'
                                 'azurehdinsight.net.\nIf this error persists, contact the SparkBatch team for more '
                                 'information.')

    def test_batch_env_is_valid_local_503(self):
        context = TestContext()
        context.set_local_mode(True)
        # env should not matter here
        context.hdi_domain = None
        context.hdi_user = None
        context.hdi_pw = None
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 503))
        self.assertFalse(bu.batch_env_is_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                                 'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_env_is_valid_local_exc(self):
        context = TestContext()
        context.set_local_mode(True)
        # env should not matter here
        context.hdi_domain = None
        context.hdi_user = None
        context.hdi_pw = None
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           requests.exceptions.ConnectionError())
        self.assertFalse(bu.batch_env_is_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'AML Batch is not currently installed on http://localhost:8080. '
                                 'Please install the app.')

    def test_batch_env_is_valid_remote_exc(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           requests.exceptions.ConnectionError())
        self.assertFalse(bu.batch_env_is_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'AML Batch is not currently installed on https://testdomain-aml.apps.'
                                 'azurehdinsight.net. Please install the app.')

    def test_batch_env_and_storage_are_valid_happy_local(self):
        context = TestContext()
        context.set_local_mode(True)
        # env should not matter here
        context.hdi_domain = None
        context.hdi_user = None
        context.hdi_pw = None
        context.az_account_key = None
        context.az_account_name = None
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 200))
        # storage should not be checked in local mode
        self.assertTrue(bu.batch_env_and_storage_are_valid(context))

    def test_batch_env_and_storage_are_valid_happy_remote(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.az_account_key = 'testaccountkey'
        context.az_account_name = 'some_storage'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 200))
        resp_json = {'Storage': [{'Value': 'some_storage'}, {'Value': 'some_more_storage'}]}
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse(json.dumps(resp_json), 200))

        self.assertTrue(bu.batch_env_and_storage_are_valid(context))

    def test_batch_env_and_storage_are_valid_env_unset_remote(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.az_account_key = None
        context.az_account_name = None
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 200))
        resp_json = {'Storage': [{'Value': 'some_storage'}, {'Value': 'some_more_storage'}]}
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse(json.dumps(resp_json), 200))

        self.assertFalse(bu.batch_env_and_storage_are_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Environment is missing the following variables:\n  AML_STORAGE_ACCT_NAME\n  '
                                 'AML_STORAGE_ACCT_KEY.\nFor help setting up environment, run\n  aml env about')

    def test_batch_env_and_storage_are_valid_exc(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.az_account_key = 'testaccountkey'
        context.az_account_name = 'some_storage'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 200))
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           requests.exceptions.ConnectionError())

        self.assertFalse(bu.batch_env_and_storage_are_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Error retrieving acceptable storage from SparkBatch: Error connecting to '
                                 'https://testdomain-aml.apps.azurehdinsight.net/v1/deploymentinfo. '
                                 'Please confirm SparkBatch app is healthy.')

    def test_batch_env_and_storage_are_valid_wrong_storage_remote(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.az_account_key = 'testaccountkey'
        context.az_account_name = 'teststorage'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_HEALTH_FMT),
                                           MockHttpResponse('', 200))
        resp_json = {'Storage': [{'Value': 'some_storage'}, {'Value': 'some_more_storage'}]}
        context.set_expected_http_response('get',
                                           bu.batch_get_url(context, bu.BATCH_DEPLOYMENT_INFO_FMT),
                                           MockHttpResponse(json.dumps(resp_json), 200))

        self.assertFalse(bu.batch_env_and_storage_are_valid(context))
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Environment storage account teststorage not found when querying server for '
                                 'acceptable storage. Available accounts are: some_storage, some_more_storage')

    def test_get_batch_job_verbose(self):
        context = TestContext()
        context.set_local_mode(True)
        service_name = 'test_service'
        job_id = 'test_job'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_SINGLE_JOB_FMT, service_name, job_id), MockHttpResponse('{}', 200))
        result = bu.batch_get_job(context, job_id, service_name, verbose=True)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(result.content, '{}')
        self.assertEqual(result.status_code, 200)
        self.assertEqual(output, 'Getting resource at http://localhost:8080/v1/webservices/test_service/jobs/test_job')

    def test_get_batch_job_conn_err(self):
        context = TestContext()
        context.set_local_mode(True)
        service_name = 'test_service'
        job_id = 'test_job'
        context.set_expected_http_response('get', bu.batch_get_url(context, bu.BATCH_SINGLE_JOB_FMT, service_name, job_id), requests.ConnectionError())
        result = bu.batch_get_job(context, job_id, service_name)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertIsNone(result)
        self.assertEqual(output, 'Error connecting to http://localhost:8080/v1/webservices/test_service/jobs/test_job. Please confirm SparkBatch app is healthy.')



if __name__ == '__main__':
    assert not hasattr(sys.stdout, "getvalue")
    unittest.main(module=__name__, buffer=True, exit=False)
