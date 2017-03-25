"""
E2e tests for batch with 
"""
import unittest
import os
import sys
import json
import uuid
from test_util import set_env_remote
from test_util import set_env_local
from test_util import set_verbosity
from test_util import print_if_verbose
from test_util import path_to_samples
from batch_util import batch_list
from batch_util import batch_list_jobs
from batch_util import batch_publish
from batch_util import batch_score
from batch_util import batch_view_service
from batch_util import batch_delete_service
from batch_util import batch_view_job
from batch_util import batch_cancel_job
from batch_util import existing_batch_service
from batch_util import existing_batch_job
from batch_util import list_job_headers
from batch_util import list_service_headers
from batch_util import view_service_headers


class BatchUnhappyPathTests(unittest.TestCase):
    environ_to_reset = {}
    aml_config_to_reset = None
    service_name = None
    local_output_folder = None
    path_to_config_file = os.path.join(os.path.expanduser('~'), '.amlconf')
    existing_service_score_args = [existing_batch_service,
                                   {
                                       '--train-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                       "azureml/jobinputs/food_inspection_trainer/"
                                                       "food_inspections1.csv",
                                       '--test-path': "https://azuremlbatchint.blob.core.windows.net/"
                                                      "azureml/jobinputs/food_inspection_trainer/"
                                                      "food_inspections2.csv"},
                                   {
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
                                   {}, False]

    def tearDown(self):
        BatchUnhappyPathTests.reset_environ()
        BatchUnhappyPathTests.reset_config()
        if BatchUnhappyPathTests.service_name is not None:

            out, err = batch_delete_service(BatchUnhappyPathTests.service_name)
            if err:
                print('Error deleting service {}: {}'.format(BatchUnhappyPathTests.service_name,
                                                             err))
                BatchUnhappyPathTests.service_name = None
        if BatchUnhappyPathTests.local_output_folder is not None:
            try:
                os.remove(BatchUnhappyPathTests.local_output_folder)
            except Exception as exc:
                print('Failure removing output {}: {}'.format(BatchUnhappyPathTests.local_output_folder, exc))
            BatchUnhappyPathTests.local_output_folder = None

    @staticmethod
    def reset_config():
        if BatchUnhappyPathTests.aml_config_to_reset is not None:
            with open(BatchUnhappyPathTests.path_to_config_file, 'w') as config_file:
                config_file.write(json.dumps(BatchUnhappyPathTests.aml_config_to_reset))
            BatchUnhappyPathTests.aml_config = None

    @staticmethod
    def reset_environ():
        for env_var in BatchUnhappyPathTests.environ_to_reset:
            os.environ[env_var] = BatchUnhappyPathTests.environ_to_reset[env_var]

        BatchUnhappyPathTests.environ_to_reset = {}

    @staticmethod
    def save_and_unset_environ_if_set(env_key):
        if env_key in os.environ:
            print_if_verbose('Unsetting {}'.format(env_key))
            BatchUnhappyPathTests.environ_to_reset[env_key] = os.environ[env_key]
            del os.environ[env_key]

    def confirm_result_with_missing_env(self, env_key_to_unset, fn_to_call, expected_output, fn_args=None, exact=True):
        fn_args = fn_args if fn_args else []
        print_if_verbose(os.environ)
        self.save_and_unset_environ_if_set(env_key_to_unset)
        result = fn_to_call(*fn_args)
        if exact:
            self.assertEqual(expected_output, result)
        else:
            self.assertTrue(expected_output in result, 'Expected {} to be in {}'.format(expected_output, result))
        self.reset_environ()

    def test_list_remote_environ_unset(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")

        self.confirm_result_with_missing_env('AML_HDI_CLUSTER', batch_list, (None, None))
        self.confirm_result_with_missing_env('AML_HDI_USER', batch_list, (None, None))
        self.confirm_result_with_missing_env('AML_HDI_PW', batch_list, (None, None))

    def test_listjobs_remote_environ_unset(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")

        self.confirm_result_with_missing_env('AML_HDI_CLUSTER', batch_list_jobs, (None, None),
                                             fn_args=[existing_batch_service])
        self.confirm_result_with_missing_env('AML_HDI_USER', batch_list_jobs, (None, None),
                                             fn_args=[existing_batch_service])
        self.confirm_result_with_missing_env('AML_HDI_PW', batch_list_jobs, (None, None),
                                             fn_args=[existing_batch_service])

    def test_view_remote_environ_unset(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")

        self.confirm_result_with_missing_env('AML_HDI_CLUSTER', batch_view_service, (None, None),
                                             fn_args=[existing_batch_service])
        self.confirm_result_with_missing_env('AML_HDI_USER', batch_view_service, (None, None),
                                             fn_args=[existing_batch_service])
        self.confirm_result_with_missing_env('AML_HDI_PW', batch_view_service, (None, None),
                                             fn_args=[existing_batch_service])

    def test_viewjob_remote_environ_unset(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")

        self.confirm_result_with_missing_env('AML_HDI_CLUSTER', batch_view_job, None,
                                             fn_args=[existing_batch_service, existing_batch_job])
        self.confirm_result_with_missing_env('AML_HDI_USER', batch_view_job, None,
                                             fn_args=[existing_batch_service, existing_batch_job])
        self.confirm_result_with_missing_env('AML_HDI_PW', batch_view_job, None,
                                             fn_args=[existing_batch_service, existing_batch_job])

    # def test_env_cluster_environ_unset_no_port_forwarding(self):
    #     with open(BatchUnhappyPathTests.path_to_config_file, 'r') as config_file:
    #         BatchUnhappyPathTests.aml_config_to_reset = json.load(config_file)
    #     to_rewrite = copy(BatchUnhappyPathTests.aml_config_to_reset)
    #     if 'port' in to_rewrite:
    #         to_rewrite['port'] = '0'
    #     with open(BatchUnhappyPathTests.path_to_config_file, 'w') as config_file:
    #         json.dump(to_rewrite, config_file)
    #
    #     self.confirm_result_with_missing_env('AML_ACS_MASTER', set_env_remote, False, fn_args=[False])

    def test_create_remote_environ_unset(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")

        self.confirm_result_with_missing_env('AML_HDI_CLUSTER', batch_publish, None,
                                             fn_args=[os.path.join(path_to_samples, 'dependency_service',
                                                                   'dependency_driver.py'), {}, {}, {}])
        self.confirm_result_with_missing_env('AML_HDI_USER', batch_publish, None,
                                             fn_args=[os.path.join(path_to_samples, 'dependency_service',
                                                                   'dependency_driver.py'), {}, {}, {}])
        self.confirm_result_with_missing_env('AML_HDI_PW', batch_publish, None,
                                             fn_args=[os.path.join(path_to_samples, 'dependency_service',
                                                                   'dependency_driver.py'), {}, {}, {}])
        self.confirm_result_with_missing_env('AML_STORAGE_ACCT_NAME', batch_publish, None,
                                             fn_args=[os.path.join(path_to_samples, 'dependency_service',
                                                                   'dependency_driver.py'), {}, {}, {}])
        self.confirm_result_with_missing_env('AML_STORAGE_ACCT_KEY', batch_publish, None,
                                             fn_args=[os.path.join(path_to_samples, 'dependency_service',
                                                                   'dependency_driver.py'), {}, {}, {}])

    def test_score_remote_environ_unset(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")

        self.confirm_result_with_missing_env('AML_HDI_CLUSTER', batch_score, None,
                                             fn_args=self.existing_service_score_args)
        self.confirm_result_with_missing_env('AML_HDI_USER', batch_score, None,
                                             fn_args=self.existing_service_score_args)
        self.confirm_result_with_missing_env('AML_HDI_PW', batch_score, None,
                                             fn_args=self.existing_service_score_args)
        self.confirm_result_with_missing_env('AML_STORAGE_ACCT_NAME', batch_score, None,
                                             fn_args=self.existing_service_score_args)
        self.confirm_result_with_missing_env('AML_STORAGE_ACCT_KEY', batch_score, None,
                                             fn_args=self.existing_service_score_args)

    def test_canceljob_remote_environ_unset(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")
        exp_string = "Environment is missing the following variables"

        self.confirm_result_with_missing_env('AML_HDI_CLUSTER', batch_cancel_job, exp_string,
                                             fn_args=[existing_batch_service, existing_batch_job],
                                             exact=False)
        self.confirm_result_with_missing_env('AML_HDI_USER', batch_cancel_job, exp_string,
                                             fn_args=[existing_batch_service, existing_batch_job],
                                             exact=False)
        self.confirm_result_with_missing_env('AML_HDI_PW', batch_cancel_job, exp_string,
                                             fn_args=[existing_batch_service, existing_batch_job],
                                             exact=False)

    def test_score_remote_invalid_storage(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")
        self.save_and_unset_environ_if_set('AML_STORAGE_ACCT_NAME')
        os.environ['AML_STORAGE_ACCT_NAME'] = 'unacceptablestorage'
        result = batch_score(*self.existing_service_score_args)
        self.assertIsNone(result)

    def test_create_remote_invalid_storage(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")
        self.save_and_unset_environ_if_set('AML_STORAGE_ACCT_NAME')
        os.environ['AML_STORAGE_ACCT_NAME'] = 'unacceptablestorage'
        result = batch_publish(os.path.join(path_to_samples, 'dependency_service', 'dependency_driver.py'), {}, {}, {})
        self.assertIsNone(result)

    def test_read_only_remote_succeeds_with_no_storage(self):
        self.assertTrue(set_env_remote(), "Unable to set ENV to cluster.")

        # create service to delete before unsetting env
        self.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})

        # start job to cancel before unsetting env
        job_id = batch_score(*self.existing_service_score_args)

        self.save_and_unset_environ_if_set('AML_STORAGE_ACCT_NAME')
        self.save_and_unset_environ_if_set('AML_STORAGE_ACCT_KEY')

        # canceljob
        result = batch_cancel_job(existing_batch_service, job_id)
        self.assertTrue('canceled' in result, 'Expected job to be canceled.')

        # delete service
        out, err = batch_delete_service(self.service_name)
        if err:
            self.fail('Unable to delete {}'.format(self.service_name))
        self.service_name = None

        # listjobs
        headers, num_entries = batch_list_jobs(existing_batch_service)
        self.assertEqual(headers, list_job_headers)
        self.assertGreaterEqual(num_entries, 0)

        # viewjob
        self.assertEqual(batch_view_job(existing_batch_service, existing_batch_job), 'Succeeded')

        # list
        headers, num_entries = batch_list()
        self.assertEqual(headers, list_service_headers)
        self.assertGreaterEqual(num_entries, 0)

        # view
        headers, num_entries = batch_view_service(existing_batch_service)
        self.assertEqual(headers, view_service_headers)

    def test_list_succeeds_with_http_cluster(self):
        self.assertTrue(set_env_remote(), 'Unable to set ENV to remote.')
        hdi_cluster = 'http://{}'.format(os.environ['AML_HDI_CLUSTER'])

        self.save_and_unset_environ_if_set('AML_HDI_CLUSTER')
        os.environ['AML_HDI_CLUSTER'] = hdi_cluster
        headers, num_entries = batch_list()
        self.assertEqual(headers, list_service_headers)
        self.assertGreaterEqual(num_entries, 0)

        os.environ['AML_HDI_CLUSTER'] = hdi_cluster.replace('http://', 'https://')
        headers, num_entries = batch_list()
        self.assertEqual(headers, list_service_headers)
        self.assertGreaterEqual(num_entries, 0)

    def test_local_succeeds_with_no_env(self):
        self.assertTrue(set_env_local(), "Unable to set ENV to local.")

        self.save_and_unset_environ_if_set('AML_HDI_CLUSTER')
        self.save_and_unset_environ_if_set('AML_HDI_USER')
        self.save_and_unset_environ_if_set('AML_HDI_PW')
        self.save_and_unset_environ_if_set('AML_STORAGE_ACCT_KEY')
        self.save_and_unset_environ_if_set('AML_STORAGE_ACCT_NAME')

        # list services
        headers, num_entries = batch_list()
        self.assertEqual(headers, list_service_headers)
        self.assertGreaterEqual(num_entries, 0)

        # create service
        self.service_name = batch_publish(
            os.path.join(path_to_samples, 'short_service', 'short_driver.py'),
            {'--input': None},
            {'--output': None},
            {})

        # view service
        headers, num_entries = batch_view_service(self.service_name)
        self.assertEqual(headers, view_service_headers)

        # score
        self.local_output_folder = os.path.join(path_to_samples,
                                                'short_service',
                                                'output{}'.format(uuid.uuid4()))
        job_state = batch_score(self.service_name,
                                inputs={'--input': os.path.join(path_to_samples,
                                                                'short_service',
                                                                'short_food_inspection.csv')},
                                outputs={'--output': self.local_output_folder},
                                parameters={})
        self.assertEqual(job_state, 'Succeeded', 'Expected job to succeed.')
        try:
            os.remove(self.local_output_folder)
        except Exception as exc:
            print('Failure removing output {}: {}'.format(self.local_output_folder, exc))

        self.local_output_folder = os.path.join(path_to_samples,
                                                'short_service',
                                                'output{}'.format(uuid.uuid4()))

        # viewjob
        job_id = batch_score(self.service_name,
                             inputs={'--input': os.path.join(path_to_samples,
                                                             'short_service',
                                                             'short_food_inspection.csv')},
                             outputs={'--output': self.local_output_folder},
                             parameters={}, wait=False)
        job_state = batch_view_job(self.service_name, job_id)
        self.assertIsNotNone(job_state)

        # canceljob
        result = batch_cancel_job(self.service_name, job_id)
        self.assertTrue('canceled' in result, 'Expected job to be canceled')
        self.local_output_folder = None

        # listjobs
        headers, num_jobs = batch_list_jobs(self.service_name)
        self.assertEqual(headers, list_job_headers)
        self.assertEqual(num_jobs, 2)

        # delete service
        out, err = batch_delete_service(self.service_name)
        self.assertTrue('deleted' in out)
        self.service_name = None


if __name__ == '__main__':
    set_verbosity('-v' in sys.argv)
    unittest.main()
