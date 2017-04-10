import copy
import json
import os
import sys
import unittest

import requests
from azure.cli.command_modules.ml.service._batchutilities import BATCH_ALL_JOBS_FMT
from azure.cli.command_modules.ml.service._batchutilities import BATCH_ALL_WS_FMT
from azure.cli.command_modules.ml.service._batchutilities import BATCH_CANCEL_JOB_FMT
from azure.cli.command_modules.ml.service._batchutilities import BATCH_DEPLOYMENT_INFO_FMT
from azure.cli.command_modules.ml.service._batchutilities import BATCH_HEALTH_FMT
from azure.cli.command_modules.ml.service._batchutilities import BATCH_SINGLE_JOB_FMT
from azure.cli.command_modules.ml.service._batchutilities import BATCH_SINGLE_WS_FMT
from azure.cli.command_modules.ml.service._batchutilities import batch_get_url
from azure.cli.command_modules.ml.service.batch import batch_cancel_job
from azure.cli.command_modules.ml.service.batch import batch_list_jobs
from azure.cli.command_modules.ml.service.batch import batch_service_create
from azure.cli.command_modules.ml.service.batch import batch_service_delete
from azure.cli.command_modules.ml.service.batch import batch_service_list
from azure.cli.command_modules.ml.service.batch import batch_service_run
from azure.cli.command_modules.ml.service.batch import batch_service_view
from azure.cli.command_modules.ml.service.batch import batch_view_job
from .mocks import MockHttpResponse
from .mocks import TestContext


class BatchUnitTests(unittest.TestCase):
    test_location = os.path.split(os.path.realpath(__file__))[0]
    relative_path_to_resources = os.path.join('..', 'test_resources')
    path_to_resources = os.path.join(test_location, relative_path_to_resources)
    path_to_empty_resource = os.path.join(test_location, 'empty_resource')
    service_name = 'test_service'
    job_id = 'test_job'
    service_payload = {"Id": service_name, "Title": service_name,
                       "Assets": [{"Id": "short_driver.py",
                                   "Uri": "driver_loc"}],
                       "Parameters": [{"Id": "--input",
                                       "IsRuntime": True,
                                       "IsOptional": False,
                                       "Kind": "Reference",
                                       "Direction": "Input"},
                                      {"Id": "--output",
                                       "IsRuntime": True,
                                       "IsOptional": False,
                                       "Kind": "Reference",
                                       "Direction": "Output"}],
                       "PackageType": "Spark20",
                       "Package": {"DriverProgramAsset": "short_driver.py",
                                   "JarAssets": [],
                                   "PythonAssets": [],
                                   "FileAssets": [],
                                   "Configuration": {},
                                   "Repositories": [],
                                   "ArchiveAssets": [],
                                   "Packages": []}}

    job_payload = {
        "WebServiceId": service_name,
        "JobId": job_id,
        "WebServiceDefinition": {
            "Id": service_name,
            "Title": service_name,
            "Assets": [{
                "Id": "short_driver.py",
                "Uri": "file:///home/stcob/.azuremlcli/dependencies/89110f07-7f23-48e6-86af-23d8f001832c/short_driver.py"
            }
            ],
            "Parameters": [{
                "Id": "--input",
                "IsRuntime": True,
                "IsOptional": False,
                "Kind": "Reference",
                "Direction": "Input"
            }, {
                "Id": "--output",
                "IsRuntime": True,
                "IsOptional": False,
                "Kind": "Reference",
                "Direction": "Output"
            }
            ],
            "PackageType": "Spark20",
            "Package": {
                "DriverProgramAsset": "short_driver.py",
                "JarAssets": [],
                "PythonAssets": [],
                "FileAssets": [],
                "Configuration": {},
                "Repositories": [],
                "ArchiveAssets": [],
                "Packages": []
            }
        },
        "JobRequest": {
            "Parameters": {
                "--input": "file:///home/stcob/.azuremlcli/parameters8135a43f-bd1e-4856-9f8d-36bae2a1d6e2/short_food_inspection.csv",
                "--output": "file:///home/stcob/notebooks/bugbash/amlbdcli/samples/short_service/outputd50801f2-26a3-4bbe-a2b7-32a07b513e31"
            }
        },
        "State": "Succeeded",
        "DriverLogFile": "/azureml/logs/testserviced607c2bb-a1ec-4ca9-9e91-b0a7ff8df4b8/2017-02-07_215853/driverlogs.log"
    }

    error_json = json.dumps({'error': {'details': [{'message': 'An error message.'}]}})

    # ================= #
    # BATCH_SERVICE_RUN #
    # ================= #

    def test_batch_service_run_invalid_env(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_service_run(service_name=self.service_name,
                          verb=False,
                          inputs=['--input-data:stuff'],
                          outputs=[],
                          parameters=[],
                          job_name=self.job_id,
                          wait_for_completion=False,
                          context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                         'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_service_run_happy_async(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        score_resp = MockHttpResponse(json.dumps(self.job_payload), 200)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           score_resp
                                           )
        batch_service_run(service_name=self.service_name,
                          verb=False,
                          inputs=[
                              '--input-data:{}'.format(self.path_to_resources, '0.txt')],
                          outputs=[],
                          parameters=[],
                          job_name=self.job_id,
                          wait_for_completion=False,
                          context=context)

        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Job test_job submitted on service test_service.\n'
                         'To check job status, run: az ml service viewjob batch -n test_service -j test_job')

    def test_batch_service_run_400_async(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        score_resp = MockHttpResponse(self.error_json, 400)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           score_resp
                                           )
        batch_service_run(service_name=self.service_name,
                          verb=False,
                          inputs=[
                              '--input-data:{}'.format(self.path_to_resources, '0.txt')],
                          outputs=[],
                          parameters=[],
                          job_name=self.job_id,
                          wait_for_completion=False,
                          context=context)

        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Failed.\nResponse code: 400\nAn error message.'
                         )

    def test_batch_service_run_conn_err_async(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           requests.ConnectionError()
                                           )
        batch_service_run(service_name=self.service_name,
                          verb=False,
                          inputs=[
                              '--input-data:{}'.format(self.path_to_resources, '0.txt')],
                          outputs=[],
                          parameters=[],
                          job_name=self.job_id,
                          wait_for_completion=False,
                          context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices/'
                         'test_service/jobs/test_job. Please confirm SparkBatch '
                         'app is healthy.'
                         )

    def test_batch_service_run_happy_sync(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        score_resp = MockHttpResponse(json.dumps(self.job_payload), 200)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           score_resp
                                           )
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           score_resp
                                           )
        batch_service_run(service_name=self.service_name,
                          verb=False,
                          inputs=[
                              '--input-data:{}'.format(self.path_to_resources, '0.txt')],
                          outputs=[],
                          parameters=[],
                          job_name=self.job_id,
                          wait_for_completion=True,
                          context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Job test_job submitted on service test_service.\n'
                         'Succeeded\n'
                         'Name: test_service\n'
                         'JobId: test_job\n'
                         'Logs available at: /azureml/logs/testserviced607c2bb-a1ec-4ca9-9e91-b0a7ff8df4b8/2017-02-07_215853/driverlogs.log\n'
                         'State: Succeeded')

    def test_batch_service_run_happy_yarn_sync(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        context.az_account_name = 'testaccount'
        context.az_account_key = 'testkey'
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        deployment_info = MockHttpResponse(
            json.dumps({'Storage': [{'Value': 'testaccount'}]}), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context,
                                                         BATCH_DEPLOYMENT_INFO_FMT),
                                           deployment_info)
        score_resp = MockHttpResponse(json.dumps(self.job_payload), 200)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           score_resp
                                           )
        view_payload = copy.copy(self.job_payload)
        view_payload['YarnAppId'] = 'YarnId'
        view_resp = MockHttpResponse(json.dumps(view_payload), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           view_resp
                                           )
        batch_service_run(service_name=self.service_name,
                          verb=False,
                          inputs=[
                              '--input-data:{}'.format(self.path_to_resources, '0.txt')],
                          outputs=[],
                          parameters=[],
                          job_name=self.job_id,
                          wait_for_completion=True,
                          context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Job test_job submitted on service test_service.\n'
                         'YarnId: Succeeded\n'
                         'Name: test_service\n'
                         'JobId: test_job\n'
                         'YarnAppId: YarnId\n'
                         'Logs available at: https://testdomain.azurehdinsight.net/yarnui/hn/cluster/app/YarnId\nState: Succeeded')

    def test_batch_service_run_400_sync(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        score_resp = MockHttpResponse(json.dumps(self.job_payload), 200)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           score_resp
                                           )
        view_resp = MockHttpResponse(self.error_json, 400)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           view_resp
                                           )

        batch_service_run(service_name=self.service_name,
                          verb=False,
                          inputs=[
                              '--input-data:{}'.format(self.path_to_resources, '0.txt')],
                          outputs=[],
                          parameters=[],
                          job_name=self.job_id,
                          wait_for_completion=True,
                          context=context)

        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Job test_job submitted on service test_service.\n'
                         'Failed.\n'
                         'Response code: 400\n'
                         'An error message.')

    # ============== #
    # BATCH_VIEW_JOB #
    # ============== #

    def test_batch_view_job_local_no_app(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_view_job(service_name=self.service_name,
                       job_name=self.job_id,
                       verb=False,
                       context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                         'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_view_job_local_happy(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        view_resp = MockHttpResponse(json.dumps(self.job_payload), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           view_resp
                                           )
        batch_view_job(service_name=self.service_name,
                       job_name=self.job_id,
                       verb=False,
                       context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Name: test_service\n'
                         'JobId: test_job\n'
                         'Logs available at: /azureml/logs/testserviced607c2bb-a1ec-4ca9-9e91-b0a7ff8df4b8/2017-02-07_215853/driverlogs.log\n'
                         'State: Succeeded')

    def test_batch_view_job_local_400(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        view_resp = MockHttpResponse(self.error_json, 400)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           view_resp
                                           )
        batch_view_job(service_name=self.service_name,
                       job_name=self.job_id,
                       verb=False,
                       context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Failed.\n'
                         'Response code: 400\n'
                         'An error message.')

    def test_batch_view_job_local_conn_err(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           requests.ConnectionError()
                                           )
        batch_view_job(service_name=self.service_name,
                       job_name=self.job_id,
                       verb=False,
                       context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices/'
                         'test_service/jobs/test_job. Please confirm SparkBatch app is '
                         'healthy.\nResponse was None.')

    # ================== #
    # BATCH_SERVICE_VIEW #
    # ================== #
    def test_batch_service_view_local_no_app(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_service_view(service_name=self.service_name,
                           verb=False,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                         'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_service_view_local_404(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        view_service_resp = MockHttpResponse(
            json.dumps({"error": {"code": "NotFound", "message": "", "target": None,
                                  "details": [{"code": "WebServiceNotFound",
                                               "message": "No web service found with id={}.".format(
                                                   self.service_name),
                                               "target": None, "details": []}]}}), 404)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           view_service_resp)
        batch_service_view(service_name=self.service_name,
                           verb=False,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Failed.\nResponse code: 404\nNo web service found with id={}.'.format(
                             self.service_name))

    def test_batch_service_view_local_happy(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        view_service_resp = MockHttpResponse(json.dumps(self.service_payload), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           view_service_resp)
        batch_service_view(service_name=self.service_name,
                           verb=False,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, '+--------------+-----------------------+\n'
                                 '| NAME         | ENVIRONMENT           |\n'
                                 '|--------------+-----------------------|\n'
                                 '| test_service | http://localhost:8080 |\n'
                                 '+--------------+-----------------------+\n'
                                 '+-----------------------------------------------------------------+----------+-----------+--------------+\n'
                                 '| SCORING_URL                                                     | INPUTS   | OUTPUTS   | PARAMETERS   |\n'
                                 '|-----------------------------------------------------------------+----------+-----------+--------------|\n'
                                 '| http://localhost:8080/v1/webservices/test_service/jobs/<job_id> | --input  | --output  |              |\n'
                                 '+-----------------------------------------------------------------+----------+-----------+--------------+\n'
                                 '\n'
                                 'Usage: az ml service run batch -n test_service --in=--input:<value> --out=--output:<value> [-w] [-j <job_id>] [-v]')

    def test_batch_service_view_remote_happy(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        view_service_resp = MockHttpResponse(json.dumps(self.service_payload), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           view_service_resp)
        batch_service_view(service_name=self.service_name,
                           verb=False,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         '+--------------+------------------------------------------------+\n'
                         '| NAME         | ENVIRONMENT                                    |\n'
                         '|--------------+------------------------------------------------|\n'
                         '| test_service | https://testdomain-aml.apps.azurehdinsight.net |\n'
                         '+--------------+------------------------------------------------+\n'
                         '+------------------------------------------------------------------------------------------+----------+-----------+--------------+\n'
                         '| SCORING_URL                                                                              | INPUTS   | OUTPUTS   | PARAMETERS   |\n'
                         '|------------------------------------------------------------------------------------------+----------+-----------+--------------|\n'
                         '| https://testdomain-aml.apps.azurehdinsight.net/v1/webservices/test_service/jobs/<job_id> | --input  | --output  |              |\n'
                         '+------------------------------------------------------------------------------------------+----------+-----------+--------------+\n'
                         '\n'
                         'Usage: az ml service run batch -n test_service --in=--input:<value> --out=--output:<value> [-w] [-j <job_id>] [-v]'
                         )

    def test_batch_service_view_remote_no_app(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_service_view(service_name=self.service_name,
                           verb=False,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Unexpected return code 503 when querying AzureBatch at '
                                 'https://testdomain-aml.apps.azurehdinsight.net.\n'
                                 'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_service_view_remote_no_env(self):
        context = TestContext()
        context.set_local_mode(False)
        batch_service_view(service_name=self.service_name,
                           verb=False,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Environment is missing the following variables:\n'
                                 '  AML_HDI_CLUSTER\n'
                                 '  AML_HDI_USER\n'
                                 '  AML_HDI_PW\n'
                                 'For help setting up environment, run\n'
                                 '  az ml env about')

    def test_batch_service_view_local_400(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        view_service_resp = MockHttpResponse(self.error_json, 400)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           view_service_resp)
        batch_service_view(service_name=self.service_name,
                           verb=False,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Failed.\nResponse code: 400\nAn error message.')

    def test_batch_service_view_local_connection_err(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           requests.exceptions.ConnectionError())
        batch_service_view(service_name=self.service_name,
                           verb=False,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices/test_service. Please confirm SparkBatch app is healthy.')

    def test_batch_service_view_local_happy_verbose(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        view_service_resp = MockHttpResponse(json.dumps(self.service_payload), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           view_service_resp)
        batch_service_view(service_name=self.service_name,
                           verb=True,
                           context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        service_json = json.loads(output.split('+')[0])
        self.assertEqual(service_json, self.service_payload)
        table = '+' + '+'.join(output.split('+')[1:])
        self.assertEqual(table, '+--------------+-----------------------+\n'
                                '| NAME         | ENVIRONMENT           |\n'
                                '|--------------+-----------------------|\n'
                                '| test_service | http://localhost:8080 |\n'
                                '+--------------+-----------------------+\n'
                                '+-----------------------------------------------------------------+----------+-----------+--------------+\n'
                                '| SCORING_URL                                                     | INPUTS   | OUTPUTS   | PARAMETERS   |\n'
                                '|-----------------------------------------------------------------+----------+-----------+--------------|\n'
                                '| http://localhost:8080/v1/webservices/test_service/jobs/<job_id> | --input  | --output  |              |\n'
                                '+-----------------------------------------------------------------+----------+-----------+--------------+\n'
                                '\n'
                                'Usage: az ml service run batch -n test_service --in=--input:<value> --out=--output:<value> [-w] [-j <job_id>] [-v]')

    # ================== #
    # BATCH_SERVICE_LIST #
    # ================== #
    def test_batch_service_list_local_no_app(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_service_list(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                         'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_service_list_local(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        list_jobs_resp = MockHttpResponse(
            json.dumps(
                [{'Name': 'test_service', 'ModificationTimeUtc': '2016-12-15T02:13:13Z'},
                 {'Name': 'another_test_service', 'ModificationTimeUtc': 'NEVER'}]), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_ALL_WS_FMT),
                                           list_jobs_resp)
        batch_service_list(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         '+----------------------+----------------------+-----------------------+\n'
                         '| NAME                 | LAST_MODIFIED_AT     | ENVIRONMENT           |\n'
                         '|----------------------+----------------------+-----------------------|\n'
                         '| test_service         | 2016-12-15T02:13:13Z | http://localhost:8080 |\n'
                         '| another_test_service | NEVER                | http://localhost:8080 |\n'
                         '+----------------------+----------------------+-----------------------+')

    def test_batch_service_list_remote_no_app(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_service_list(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Unexpected return code 503 when querying AzureBatch at '
                                 'https://testdomain-aml.apps.azurehdinsight.net.\n'
                                 'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_service_list_remote_environ_unset(self):
        context = TestContext()
        context.set_local_mode(False)
        batch_service_list(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Environment is missing the following variables:\n'
                                 '  AML_HDI_CLUSTER\n'
                                 '  AML_HDI_USER\n'
                                 '  AML_HDI_PW\n'
                                 'For help setting up environment, run\n'
                                 '  az ml env about')

    def test_batch_service_list_remote(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        list_jobs_resp = MockHttpResponse(
            json.dumps(
                [{'Name': 'test_service', 'ModificationTimeUtc': '2016-12-15T02:13:13Z'},
                 {'Name': 'another_test_service', 'ModificationTimeUtc': 'NEVER'}]), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_ALL_WS_FMT),
                                           list_jobs_resp)
        batch_service_list(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         '+----------------------+----------------------+------------------------------------------------+\n'
                         '| NAME                 | LAST_MODIFIED_AT     | ENVIRONMENT                                    |\n'
                         '|----------------------+----------------------+------------------------------------------------|\n'
                         '| test_service         | 2016-12-15T02:13:13Z | https://testdomain-aml.apps.azurehdinsight.net |\n'
                         '| another_test_service | NEVER                | https://testdomain-aml.apps.azurehdinsight.net |\n'
                         '+----------------------+----------------------+------------------------------------------------+')

    def test_batch_service_list_connection_err(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_ALL_WS_FMT),
                                           requests.exceptions.ConnectionError())
        batch_service_list(context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices. Please confirm SparkBatch app is healthy.')

    # =============== #
    # BATCH_LIST_JOBS #
    # =============== #
    def test_batch_list_jobs_local_no_app(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_list_jobs(service_name=self.service_name,
                        context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                         'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_list_jobs_remote_no_app(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_list_jobs(service_name=self.service_name,
                        context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Unexpected return code 503 when querying AzureBatch at '
                                 'https://testdomain-aml.apps.azurehdinsight.net.\n'
                                 'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_list_jobs_local_404(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        list_jobs_resp = MockHttpResponse(
            json.dumps({"error": {"code": "NotFound", "message": "", "target": None,
                                  "details": [{"code": "WebServiceNotFound",
                                               "message": "No web service found with id={}.".format(
                                                   self.service_name),
                                               "target": None, "details": []}]}}), 404)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_ALL_JOBS_FMT,
                                                         self.service_name),
                                           list_jobs_resp)
        batch_list_jobs(service_name=self.service_name,
                        context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Failed.\nResponse code: 404\nNo web service found with id={}.'.format(
                             self.service_name))

    def test_batch_list_jobs_local_happy(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        list_jobs_resp = MockHttpResponse(
            json.dumps(
                [{'Name': 'test_job', 'ModificationTimeUtc': '2016-12-15T02:13:13Z'},
                 {'Name': 'another_test_job', 'ModificationTimeUtc': 'NEVER'}]), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_ALL_JOBS_FMT,
                                                         self.service_name),
                                           list_jobs_resp)
        batch_list_jobs(service_name=self.service_name,
                        context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         '+------------------+----------------------+-----------------------+\n'
                         '| NAME             | LAST_MODIFIED_AT     | ENVIRONMENT           |\n'
                         '|------------------+----------------------+-----------------------|\n'
                         '| test_job         | 2016-12-15T02:13:13Z | http://localhost:8080 |\n'
                         '| another_test_job | NEVER                | http://localhost:8080 |\n'
                         '+------------------+----------------------+-----------------------+')

    def test_batch_list_jobs_remote_environ_unset(self):
        context = TestContext()
        context.set_local_mode(False)
        batch_list_jobs(service_name=self.service_name,
                        context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Environment is missing the following variables:\n'
                                 '  AML_HDI_CLUSTER\n'
                                 '  AML_HDI_USER\n'
                                 '  AML_HDI_PW\n'
                                 'For help setting up environment, run\n'
                                 '  az ml env about')

    def test_batch_list_jobs_remote_happy(self):
        context = TestContext()
        context.set_local_mode(False)
        context.hdi_domain = 'testdomain'
        context.hdi_user = 'testuser'
        context.hdi_pw = 'testpw'
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        list_jobs_resp = MockHttpResponse(
            json.dumps(
                [{'Name': 'test_job', 'ModificationTimeUtc': '2016-12-15T02:13:13Z'},
                 {'Name': 'another_test_job', 'ModificationTimeUtc': 'NEVER'}]), 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_ALL_JOBS_FMT,
                                                         self.service_name),
                                           list_jobs_resp)
        batch_list_jobs(service_name=self.service_name,
                        context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         '+------------------+----------------------+------------------------------------------------+\n'
                         '| NAME             | LAST_MODIFIED_AT     | ENVIRONMENT                                    |\n'
                         '|------------------+----------------------+------------------------------------------------|\n'
                         '| test_job         | 2016-12-15T02:13:13Z | https://testdomain-aml.apps.azurehdinsight.net |\n'
                         '| another_test_job | NEVER                | https://testdomain-aml.apps.azurehdinsight.net |\n'
                         '+------------------+----------------------+------------------------------------------------+')

    def test_batch_list_jobs_local_connection_err(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_ALL_JOBS_FMT,
                                                         self.service_name),
                                           requests.exceptions.ConnectionError())
        batch_list_jobs(service_name=self.service_name,
                        context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices/test_service/jobs. Please confirm SparkBatch app is healthy.')

    # ================ #
    # BATCH_CANCEL_JOB #
    # ================ #
    def test_batch_cancel_job_invalid_env_local(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_cancel_job(service_name=self.service_name,
                         job_name=self.job_id,
                         verb=False,
                         context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                         'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_cancel_job_invalid_env_cluster(self):
        context = TestContext()
        context.set_local_mode(False)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_cancel_job(service_name=self.service_name,
                         job_name=self.job_id,
                         verb=False,
                         context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Environment is missing the following variables:\n'
                                 '  AML_HDI_CLUSTER\n'
                                 '  AML_HDI_USER\n'
                                 '  AML_HDI_PW\n'
                                 'For help setting up environment, run\n'
                                 '  az ml env about')

    def test_batch_cancel_job_happy(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        cancellation_str = 'Job {0} of service {1} canceled.'.format(self.service_name,
                                                                     self.job_id)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        cancel_resp = MockHttpResponse(cancellation_str, 200)
        context.set_expected_http_response('post',
                                           batch_get_url(context, BATCH_CANCEL_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           cancel_resp)
        batch_cancel_job(service_name=self.service_name,
                         job_name=self.job_id,
                         verb=False,
                         context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, cancellation_str)

    def test_batch_cancel_job_verbose(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        cancellation_str = 'Job {0} of service {1} canceled.'.format(self.service_name,
                                                                     self.job_id)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        cancel_resp = MockHttpResponse(cancellation_str, 200)
        context.set_expected_http_response('post',
                                           batch_get_url(context, BATCH_CANCEL_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           cancel_resp)
        batch_cancel_job(service_name=self.service_name,
                         job_name=self.job_id,
                         verb=True,
                         context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Canceling job by posting to http://localhost:8080/v1/webservices/test_service/jobs/test_job/cancel\n'
                         'Job test_service of service test_job canceled.\n'
                         'Job test_service of service test_job canceled.')

    def test_batch_cancel_job_400(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        cancel_resp = MockHttpResponse(self.error_json, 400)
        context.set_expected_http_response('post',
                                           batch_get_url(context, BATCH_CANCEL_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           cancel_resp)
        batch_cancel_job(service_name=self.service_name,
                         job_name=self.job_id,
                         verb=False,
                         context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Failed.\nResponse code: 400\nAn error message.')

    def test_batch_cancel_job_connection_err(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('post',
                                           batch_get_url(context, BATCH_CANCEL_JOB_FMT,
                                                         self.service_name, self.job_id),
                                           requests.ConnectionError())
        batch_cancel_job(service_name=self.service_name,
                         job_name=self.job_id,
                         verb=False,
                         context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices/test_service/jobs/test_job/cancel. Please confirm SparkBatch app is healthy.')

    # ==================== #
    # BATCH_SERVICE_DELETE #
    # ==================== #
    def test_batch_service_delete_bad_env(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_service_delete(service_name=self.service_name,
                             verb=False,
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                         'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_service_delete_not_found(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse(self.error_json, 404))
        batch_service_delete(service_name=self.service_name,
                             verb=False,
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Failed.\n'
                         'Response code: 404\n'
                         'An error message.')

    def test_batch_service_delete_get_fails(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           requests.ConnectionError())
        batch_service_delete(service_name=self.service_name,
                             verb=False,
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices/test_service. Please confirm SparkBatch app is healthy.')

    def test_batch_service_delete_conn_err(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse('{}', 200))
        context.set_expected_http_response('delete',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           requests.ConnectionError())
        batch_service_delete(service_name=self.service_name,
                             verb=False,
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices/test_service. Please confirm SparkBatch app is healthy.')

    def test_batch_service_delete_happy(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse('{}', 200))
        context.set_expected_http_response('delete',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse('', 200))
        batch_service_delete(service_name=self.service_name,
                             verb=False,
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Service test_service deleted.')

    def test_batch_service_delete_happy_verbose(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse('{}', 200))
        context.set_expected_http_response('delete',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse('', 200))
        batch_service_delete(service_name=self.service_name,
                             verb=True,
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         '{}\n'
                         'Deleting resource at http://localhost:8080/v1/webservices/test_service\n'
                         '\n'
                         'Service test_service deleted.')

    # ==================== #
    # BATCH_SERVICE_CREATE #
    # ==================== #

    def test_batch_service_create_bad_env(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 503)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        batch_service_create(driver_file='driver.py',
                             service_name=self.service_name,
                             title=None,
                             verb=False,
                             inputs=[],
                             outputs=[],
                             parameters=[],
                             dependencies=[],
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Unexpected return code 503 when querying AzureBatch at http://localhost:8080.\n'
                         'If this error persists, contact the SparkBatch team for more information.')

    def test_batch_service_create_nonexistent_dependency(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse(
                                               json.dumps(self.service_payload), 200))
        batch_service_create(driver_file='driver.py',
                             service_name=self.service_name,
                             title=None,
                             verb=False,
                             inputs=['--input-data'],
                             outputs=['--output-data'],
                             parameters=['--parameter'],
                             dependencies=['blah.txt'],
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(output.startswith('Error uploading dependencies'))

    def test_batch_service_create_nonexistent_driver(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse(
                                               json.dumps(self.service_payload), 200))
        batch_service_create(driver_file='driver.py',
                             service_name=self.service_name,
                             title='a_title',
                             verb=False,
                             inputs=['--input-data'],
                             outputs=['--output-data'],
                             parameters=['--parameter'],
                             dependencies=[],
                             context=context)

        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(output.startswith('Error uploading driver'))

    def test_batch_service_create_happy_no_defaults(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse(
                                               json.dumps(self.service_payload), 200))
        batch_service_create(driver_file=os.path.join(self.path_to_resources, '0.txt'),
                             service_name=self.service_name,
                             title='a_title',
                             verb=False,
                             inputs=['--input-data'],
                             outputs=['--output-data'],
                             parameters=['--parameter'],
                             dependencies=[os.path.join(self.path_to_resources, '1.txt')],
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Success.\n'
                                 '+--------------+-----------------------+\n'
                                 '| NAME         | ENVIRONMENT           |\n'
                                 '|--------------+-----------------------|\n'
                                 '| test_service | http://localhost:8080 |\n'
                                 '+--------------+-----------------------+\n\n'
                                 'Usage: az ml service run batch -n test_service --in=--input-data:<value> '
                                 '--param=--parameter:<value> --out=--output-data:<value> [-w] [-j <job_id>] [-v]')

    def test_batch_service_create_happy_defaults(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse(
                                               json.dumps(self.service_payload), 200))
        batch_service_create(driver_file=os.path.join(self.path_to_resources, '0.txt'),
                             service_name=self.service_name,
                             title='a_title',
                             verb=False,
                             inputs=['--input-data:{}'.format(os.path.join(self.path_to_resources, '1.txt'))],
                             outputs=['--output-data'],
                             parameters=['--parameter'],
                             dependencies=[os.path.join(self.path_to_resources, '1.txt')],
                             context=context)

        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, 'Success.\n'
                                 '+--------------+-----------------------+\n'
                                 '| NAME         | ENVIRONMENT           |\n'
                                 '|--------------+-----------------------|\n'
                                 '| test_service | http://localhost:8080 |\n'
                                 '+--------------+-----------------------+\n\n'
                                 'Usage: az ml service run batch -n test_service '
                                 '--param=--parameter:<value> --out=--output-data:<value> '
                                 '[--in=--input-data:<value>] [-w] [-j <job_id>] [-v]')

    def test_batch_service_create_nonexistent_defaults(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           MockHttpResponse(
                                               json.dumps(self.service_payload), 200))
        batch_service_create(driver_file=os.path.join(self.path_to_resources, '0.txt'),
                             service_name=self.service_name,
                             title='a_title',
                             verb=False,
                             inputs=['--input-data:blah.txt'],
                             outputs=['--output-data'],
                             parameters=['--parameter'],
                             dependencies=[os.path.join(self.path_to_resources, '1.txt')],
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(output.startswith('Error creating parameter list'))

    def test_batch_service_create_conn_err(self):
        context = TestContext()
        context.set_local_mode(True)
        health_resp = MockHttpResponse('{}', 200)
        context.set_expected_http_response('get',
                                           batch_get_url(context, BATCH_HEALTH_FMT),
                                           health_resp)
        context.set_expected_http_response('put',
                                           batch_get_url(context, BATCH_SINGLE_WS_FMT,
                                                         self.service_name),
                                           requests.ConnectionError())
        batch_service_create(driver_file=os.path.join(self.path_to_resources, '0.txt'),
                             service_name=self.service_name,
                             title='a_title',
                             verb=False,
                             inputs=['--input-data'],
                             outputs=['--output-data'],
                             parameters=['--parameter'],
                             dependencies=[os.path.join(self.path_to_resources, '1.txt')],
                             context=context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output,
                         'Error connecting to http://localhost:8080/v1/webservices/test_service. '
                         'Please confirm SparkBatch app is healthy.')


if __name__ == '__main__':
    assert not hasattr(sys.stdout, "getvalue")
    unittest.main(module=__name__, buffer=True, exit=False)
