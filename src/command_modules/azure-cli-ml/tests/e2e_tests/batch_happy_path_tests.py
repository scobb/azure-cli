"""
Batch CLI e2e tests
"""
import unittest
import os
import sys
import uuid
import json
from batch_util import batch_publish
from batch_util import batch_score
from batch_util import existing_batch_service
from batch_util import existing_batch_job
from batch_util import batch_view_job
from batch_util import batch_list
from batch_util import batch_list_jobs
from batch_util import batch_view_service
from batch_util import batch_cancel_job
from batch_util import batch_delete_service
from batch_util import list_job_headers
from batch_util import list_service_headers
from batch_util import view_service_headers
from test_util import LOCAL
from test_util import REMOTE
from test_util import env_name_to_func
from test_util import path_to_samples
from test_util import set_verbosity
from test_util import print_if_verbose
from test_util import aml_env
from test_util import set_env_remote
from test_util import test_location
from test_util import relative_path_to_samples


class BatchHappyPathTests(unittest.TestCase):
    service_name = None
    local_output_folder = None
    old_pwd = None

    def tearDown(self):
        if BatchHappyPathTests.service_name is not None:
            out, err = batch_delete_service(BatchHappyPathTests.service_name)
            if err:
                print('Error deleting service {}: {}'.format(BatchHappyPathTests.service_name,
                                                             err))
            BatchHappyPathTests.service_name = None
        if BatchHappyPathTests.local_output_folder is not None:
            try:
                os.remove(BatchHappyPathTests.local_output_folder)
            except Exception as exc:
                print('Failure removing output {}: {}'.format(BatchHappyPathTests.local_output_folder, exc))
            BatchHappyPathTests.local_output_folder = None
        # TODO - cleanup remote output

        if BatchHappyPathTests.old_pwd is not None:
            os.chdir(BatchHappyPathTests.old_pwd)
            BatchHappyPathTests.old_pwd = None

    # TODO - separate in remote and local tests
    def validate_json_contract(self, json_str, expected_keys=None):
        if json_str is None:
            self.fail('Unexpected response from CLI')
        if expected_keys is None:
            expected_keys = []
        try:
            result_json = json.loads(json_str)
            # TODO - more extensive contract checking
            for key in expected_keys:
                if key not in result_json:
                    self.fail('Expected {} in json contact {}'.format(key, result_json))
            return result_json
        except:
            self.fail('Error parsing json.')

    def validate_json_list(self, json_str):
        if json_str is None:
            self.fail('Unexpected response from CLI')
        try:
            result_json = json.loads(json_str)
            self.assertTrue(isinstance(result_json, list))
        except:
            print(json_str)
            self.fail('Result was not a json string.')

    def set_env(self, env_name):
        aml_env()
        if not env_name_to_func[env_name]():
            self.fail('Failure setting env to {}.'.format(env_name))

    def test_batch_view_service_remote(self):
        self.set_env(REMOTE)
        headers, entries = batch_view_service(existing_batch_service)
        self.assertEqual(view_service_headers, headers, "Expected all headers to be present in response.")

    def test_batch_view_service_local(self):
        self.set_env(LOCAL)
        # publish service

        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'food_inspection_trainer', 'driver.py'),
            {'--train-path': None, '--test-path': None},
            {'--eval-results-path': None, '--model-output-path': None},
            {})
        print_if_verbose(BatchHappyPathTests.service_name)
        self.assertIsNotNone(BatchHappyPathTests.service_name)

        headers, entries = batch_view_service(BatchHappyPathTests.service_name)
        self.assertEqual(view_service_headers, headers, "Expected all headers to be present in response.")

    def test_batch_viewjob_remote(self):
        self.set_env(REMOTE)
        job_state = batch_view_job(existing_batch_service, existing_batch_job)
        self.assertIsNotNone(job_state)

    def test_batch_viewjob_local(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})
        self.assertIsNotNone(BatchHappyPathTests.service_name, 'Failed to publish service.')
        job_id = batch_score(BatchHappyPathTests.service_name,
                             inputs={'--input': os.path.join(path_to_samples,
                                                             'short_service',
                                                             'short_food_inspection.csv')},
                             outputs={'--output': os.path.join(path_to_samples,
                                                               'short_service',
                                                               'output{}'.format(
                                                                   uuid.uuid4()))},
                             parameters={},
                             wait=False)
        self.assertIsNotNone(job_id, 'Failed to start a job.')
        job_state = batch_view_job(BatchHappyPathTests.service_name, job_id)
        self.assertIsNotNone(job_state)

    def test_batch_publish_local(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'food_inspection_trainer', 'driver.py'),
            {'--train-path': None, '--test-path': None},
            {'--eval-results-path': None, '--model-output-path': None},
            {})
        print_if_verbose(BatchHappyPathTests.service_name)
        self.assertIsNotNone(BatchHappyPathTests.service_name)

    def test_batch_publish_remote(self):
        self.set_env(REMOTE)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'food_inspection_trainer', 'driver.py'),
            {'--train-path': None, '--test-path': None},
            {'--eval-results-path': None, '--model-output-path': None},
            {})
        print_if_verbose(BatchHappyPathTests.service_name)
        self.assertIsNotNone(BatchHappyPathTests.service_name)

    def batch_score_common(self, env_setup_fn, service_name, inputs, outputs,
                           parameters, wait=True):
        if not env_setup_fn():
            self.fail('Failure setting env using {}'.format(env_setup_fn))
        return batch_score(service_name, inputs, outputs, parameters, wait=wait)

    def test_batch_service_list_remote(self):
        self.set_env(REMOTE)
        headers, num_entries = batch_list()
        self.assertEqual(headers, list_service_headers)
        self.assertGreaterEqual(num_entries, 1)

    def test_batch_service_list_local(self):
        self.set_env(LOCAL)
        headers, num_entries = batch_list()
        self.assertEqual(headers, list_service_headers)
        self.assertGreaterEqual(num_entries, 1)

    def test_batch_score_synch_remote_existing_with_local_parameters(self):
        job_state = self.batch_score_common(set_env_remote,
                                            existing_batch_service,
                                            inputs={
                                                '--train-path': os.path.join(path_to_samples, 'food_inspection_trainer',
                                                                             'food_inspections1.csv'),
                                                '--test-path': os.path.join(path_to_samples, 'food_inspection_trainer',
                                                                            "food_inspections2.csv")},
                                            outputs={
                                                '--eval-results-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                       'net/azureml/joboutputs/'
                                                                       'food_inspection_trainer/'
                                                                       'eval_results{}.parquet'.format(
                                                    uuid.uuid4()),
                                                '--model-output-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                       'net/azureml/joboutputs/'
                                                                       'food_inspection_trainer/'
                                                                       'trained_model{}.model'.format(
                                                    uuid.uuid4())},
                                            parameters={})
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_batch_publish_and_score_synch_local(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})
        self.assertIsNotNone(BatchHappyPathTests.service_name)
        BatchHappyPathTests.local_output_folder = os.path.join(path_to_samples,
                                                      'short_service',
                                                      'output{}'.format(uuid.uuid4()))
        job_state = batch_score(BatchHappyPathTests.service_name,
                                inputs={'--input': os.path.join(path_to_samples,
                                                                'short_service',
                                                                'short_food_inspection.csv')},
                                outputs={'--output': BatchHappyPathTests.local_output_folder},
                                parameters={})
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_batch_publish_and_score_relative_path_synch_local(self):
        BatchHappyPathTests.old_pwd = os.getcwd()
        os.chdir(test_location)
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(relative_path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})
        self.assertIsNotNone(BatchHappyPathTests.service_name)
        BatchHappyPathTests.local_output_folder = os.path.join(relative_path_to_samples,
                                                      'short_service',
                                                      'output{}'.format(uuid.uuid4()))
        job_state = batch_score(BatchHappyPathTests.service_name,
                                inputs={'--input': os.path.join(relative_path_to_samples,
                                                                'short_service',
                                                                'short_food_inspection.csv')},
                                outputs={'--output': BatchHappyPathTests.local_output_folder},
                                parameters={})
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_batch_publish_and_score_dependencies_sync_local(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'dependency_service', 'dependency_driver.py'),
            {}, {}, {},
            dependencies=[os.path.join(path_to_samples, 'dependency_service',
                                       'my_dependency.py')])
        self.assertIsNotNone(BatchHappyPathTests.service_name)
        job_state = batch_score(BatchHappyPathTests.service_name,
                                inputs={}, outputs={}, parameters={},
                                wait=True)
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_batch_publish_and_score_dependencies_sync_remote(self):
        self.set_env(REMOTE)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'dependency_service', 'dependency_driver.py'),
            {}, {}, {},
            dependencies=[os.path.join(path_to_samples, 'dependency_service',
                                       'my_dependency.py')])
        self.assertIsNotNone(BatchHappyPathTests.service_name)
        job_state = batch_score(BatchHappyPathTests.service_name,
                                inputs={}, outputs={}, parameters={},
                                wait=True)
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_batch_publish_and_score_async_local(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})
        self.assertIsNotNone(BatchHappyPathTests.service_name)
        job_id = batch_score(BatchHappyPathTests.service_name,
                             inputs={'--input': os.path.join(path_to_samples,
                                                             'short_service',
                                                             'short_food_inspection.csv')},
                             outputs={'--output': os.path.join(path_to_samples,
                                                               'short_service',
                                                               'output{}'.format(
                                                                   uuid.uuid4()))},
                             parameters={},
                             wait=False)
        self.assertIsNotNone(job_id, 'Did not receive job id.')
        # TODO - find a good way to remove this

    def test_batch_score_async_remote_existing(self):
        job_id = self.batch_score_common(set_env_remote,
                                         existing_batch_service,
                                         inputs={
                                             '--train-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                             "azureml/jobinputs/food_inspection_trainer/"
                                                             "food_inspections1.csv",
                                             '--test-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                            "azureml/jobinputs/food_inspection_trainer/"
                                                            "food_inspections2.csv"},
                                         outputs={
                                             '--eval-results-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                    'net/azureml/joboutputs/'
                                                                    'food_inspection_trainer/'
                                                                    'eval_results{}.parquet'.format(
                                                 uuid.uuid4()),
                                             '--model-output-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                    'net/azureml/joboutputs/'
                                                                    'food_inspection_trainer/'
                                                                    'trained_model{}.model'.format(
                                                 uuid.uuid4())},
                                         parameters={},
                                         wait=False)
        self.assertIsNotNone(job_id, 'Failure starting async job.')

    def test_batch_score_synch_remote_existing(self):
        job_state = self.batch_score_common(set_env_remote,
                                            existing_batch_service,
                                            inputs={
                                                '--train-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                                "azureml/jobinputs/food_inspection_trainer/"
                                                                "food_inspections1.csv",
                                                '--test-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                               "azureml/jobinputs/food_inspection_trainer/"
                                                               "food_inspections2.csv"},
                                            outputs={
                                                '--eval-results-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                       'net/azureml/joboutputs/'
                                                                       'food_inspection_trainer/'
                                                                       'eval_results{}.parquet'.format(
                                                    uuid.uuid4()),
                                                '--model-output-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                       'net/azureml/joboutputs/'
                                                                       'food_inspection_trainer/'
                                                                       'trained_model{}.model'.format(
                                                    uuid.uuid4())},
                                            parameters={})
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_batch_create_and_score_synch_local_defaults(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.local_output_folder = os.path.join(path_to_samples,
                                                      'short_service',
                                                      'output{}'.format(uuid.uuid4()))
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': os.path.join(path_to_samples, 'short_service',
                                     'short_food_inspection.csv')},
            {'--output': BatchHappyPathTests.local_output_folder},
            {})
        self.assertIsNotNone(BatchHappyPathTests.service_name)
        job_state = batch_score(BatchHappyPathTests.service_name,
                                inputs={},
                                outputs={},
                                parameters={})
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_batch_create_and_score_synch_remote_defaults(self):
        self.set_env(REMOTE)

        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'food_inspection_trainer', 'driver.py'),
            {'--train-path': None,
             '--test-path': os.path.join(path_to_samples, 'food_inspection_trainer', 'food_inspections2.csv')},
            {'--eval-results-path': None, '--model-output-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                 'net/azureml/joboutputs/'
                                                                 'food_inspection_trainer/'
                                                                 'trained_model{}.model'.format(uuid.uuid4())},
            {})

        self.assertIsNotNone(BatchHappyPathTests.service_name)

        job_state = self.batch_score_common(set_env_remote,
                                            BatchHappyPathTests.service_name,
                                            inputs={
                                                '--train-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                                "azureml/jobinputs/food_inspection_trainer/"
                                                                "food_inspections1.csv",
                                                '--test-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                               "azureml/jobinputs/food_inspection_trainer/"
                                                               "food_inspections2.csv"},
                                            outputs={
                                                '--eval-results-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                       'net/azureml/joboutputs/'
                                                                       'food_inspection_trainer/'
                                                                       'eval_results{}.parquet'.format(
                                                    uuid.uuid4())},
                                            parameters={}, wait=True)
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_list_jobs_remote(self):
        self.set_env(REMOTE)
        headers, num_entries = batch_list_jobs(existing_batch_service)
        self.assertEqual(headers, list_job_headers)
        self.assertGreater(num_entries, 0)

    def test_list_jobs_local(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})
        if BatchHappyPathTests.service_name is None:
            self.fail('Failure publishing dummy service.')

        headers, num_entries = batch_list_jobs(BatchHappyPathTests.service_name)
        self.assertEqual(headers, list_job_headers)
        self.assertEqual(num_entries, 0)

    def test_cancel_job_local(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})
        self.assertIsNotNone(BatchHappyPathTests.service_name, 'Failed to publish service.')
        job_id = batch_score(BatchHappyPathTests.service_name,
                             inputs={'--input': os.path.join(path_to_samples,
                                                             'short_service',
                                                             'short_food_inspection.csv')},
                             outputs={'--output': os.path.join(path_to_samples,
                                                               'short_service',
                                                               'output{}'.format(
                                                                   uuid.uuid4()))},
                             parameters={},
                             wait=False)
        self.assertIsNotNone(job_id, 'Failed to start a job.')
        result = batch_cancel_job(BatchHappyPathTests.service_name, job_id)
        self.assertTrue('canceled' in result, 'Expected job to be canceled.')

    def test_cancel_job_remote(self):
        self.set_env(REMOTE)

        job_id = self.batch_score_common(set_env_remote,
                                         existing_batch_service,
                                         inputs={
                                             '--train-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                             "azureml/jobinputs/food_inspection_trainer/"
                                                             "food_inspections1.csv",
                                             '--test-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                            "azureml/jobinputs/food_inspection_trainer/"
                                                            "food_inspections2.csv"},
                                         outputs={
                                             '--eval-results-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                    'net/azureml/joboutputs/'
                                                                    'food_inspection_trainer/'
                                                                    'eval_results{}.parquet'.format(
                                                 uuid.uuid4()),
                                             '--model-output-path': 'https://azuremlbatchint.blob.core.windows.'
                                                                    'net/azureml/joboutputs/'
                                                                    'food_inspection_trainer/'
                                                                    'trained_model{}.model'.format(
                                                 uuid.uuid4())},
                                         parameters={}, wait=False)
        self.assertIsNotNone(job_id, 'Failed to start a job.')
        result = batch_cancel_job(existing_batch_service, job_id)
        self.assertTrue('canceled' in result, 'Expected job to be canceled.')

    def test_batch_publish_and_score_local_with_remote_input_paths(self):
        self.set_env(LOCAL)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})
        self.assertIsNotNone(BatchHappyPathTests.service_name, 'Failed to publish service.')
        job_id = batch_score(BatchHappyPathTests.service_name,
                             inputs={'--input': "https://azuremlbatchint.blob.core.windows.net/"
                                                "azureml/jobinputs/food_inspection_trainer/"
                                                "food_inspections1.csv"},
                             outputs={'--output': os.path.join(path_to_samples,
                                                               'short_service',
                                                               'output{}'.format(
                                                                   uuid.uuid4()))},
                             parameters={},
                             wait=False)
        self.assertIsNone(job_id,
                          'Started a job with local environment and remote input paths.')

    def test_batch_publish_and_score_remote_with_wasb_default_path(self):
        self.set_env(REMOTE)
        job_state = self.batch_score_common(set_env_remote,
                                            existing_batch_service,
                                            inputs={
                                                '--train-path': "wasb:///jobinputs/food_inspection_trainer/"
                                                                "food_inspections1.csv",
                                                '--test-path': "wasb:///jobinputs/food_inspection_trainer/"
                                                               "food_inspections2.csv"},
                                            outputs={
                                                '--eval-results-path': 'wasb:///joboutputs/'
                                                                       'food_inspection_trainer/'
                                                                       'eval_results{}.parquet'.format(uuid.uuid4()),
                                                '--model-output-path': 'wasb:///joboutputs/'
                                                                       'food_inspection_trainer/'
                                                                       'trained_model{}.model'.format(uuid.uuid4())},
                                            parameters={})
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')

    def test_batch_publish_and_score_with_local_model(self):
        self.set_env(REMOTE)
        BatchHappyPathTests.service_name = batch_publish(
            os.path.join(path_to_samples, 'food_inspection_scorer', 'driver.py'),
            {'--input-data': None, '--trained-model': None},
            {'--output-data': None},
            {})
        self.assertIsNotNone(BatchHappyPathTests.service_name, 'Service was not created correctly.')
        job_state = self.batch_score_common(set_env_remote,
                                            BatchHappyPathTests.service_name,
                                            inputs={'--input-data': os.path.join(path_to_samples,
                                                                                 'food_inspection_scorer',
                                                                                 'food_inspections2.csv'),
                                                    '--trained-model': os.path.join(path_to_samples,
                                                                                    'food_inspection_trainer',
                                                                                    'inspection.model')},
                                            outputs={'--output-data': 'wasb:///joboutputs/'
                                                                      'food_inspection_scorer/'
                                                                      'output{}.parquet'.format(uuid.uuid4())},
                                            parameters={})
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')


if __name__ == '__main__':
    if 'AML_STORAGE_ACCT_NAME' not in os.environ:
        print('Please run through batch_tests.sh.')
        print('Usage: ./batch_tests.sh <environment_file.sh> [-v] [options]')
        exit(1)
    if '-v' in sys.argv:
        set_verbosity(True)
    unittest.main()
