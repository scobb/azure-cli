import json
import time
import uuid

import requests
from pkg_resources import resource_string

from ._batchutilities import BATCH_ALL_JOBS_FMT
from ._batchutilities import BATCH_ALL_WS_FMT
from ._batchutilities import BATCH_CANCEL_JOB_FMT
from ._batchutilities import BATCH_SINGLE_JOB_FMT
from ._batchutilities import BATCH_SINGLE_WS_FMT
from ._batchutilities import batch_create_parameter_list
from ._batchutilities import batch_create_service_header_to_fn_dict
from ._batchutilities import batch_env_and_storage_are_valid
from ._batchutilities import batch_env_is_valid
from ._batchutilities import batch_get_asset_type
from ._batchutilities import batch_get_job
from ._batchutilities import batch_get_job_description
from ._batchutilities import batch_get_parameter_str
from ._batchutilities import batch_get_url
from ._batchutilities import batch_list_jobs_header_to_fn_dict
from ._batchutilities import batch_list_service_header_to_fn_dict
from ._batchutilities import batch_view_service_header_to_fn_dict
from ._batchutilities import batch_view_service_usage_header_to_fn_dict
from ._batchutilities import get_success_and_resp_str
from ._batchutilities import validate_and_split_run_param
from .._util import MultiTableResponse
from .._util import StaticStringResponse
from .._util import StaticStringWithTableReponse
from .._util import TableResponse
from .._util import cli_context
from .._util import get_json
from .._util import update_asset_path


def batch_service_list(context=cli_context):
    """
    Processing for listing existing batch services
    :param context: CommandLineInterfaceContext object
    :return: None
    """
    if not batch_env_is_valid(context):
        return
    url = batch_get_url(context, BATCH_ALL_WS_FMT)
    try:
        resp = context.http_call('get', url, auth=(context.hdi_user, context.hdi_pw))
        print(get_success_and_resp_str(context, resp, response_obj=TableResponse(
            batch_list_service_header_to_fn_dict))[1])
    except requests.ConnectionError:
        print(
        "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return


def batch_service_view(service_name, verb, context=cli_context):
    """
    Processing for viewing an existing batch service
    :param context: CommandLineInterfaceContext object
    :param verb: bool flag to indicate verbosity
    :param service_name: str name of service
    :return: None
    """
    if not batch_env_is_valid(context):
        return

    url = batch_get_url(context, BATCH_SINGLE_WS_FMT, service_name)
    try:
        resp = context.http_call('get', url, auth=(context.hdi_user, context.hdi_pw))

        success, response = get_success_and_resp_str(context, resp,
                                                     response_obj=MultiTableResponse(
                                                         [
                                                             batch_view_service_header_to_fn_dict,
                                                             batch_view_service_usage_header_to_fn_dict]),
                                                     verbose=verb)
        print(response)
        if success:
            param_str = ' '.join([batch_get_parameter_str(p) for
                                  p in sorted(resp.json()['Parameters'],
                                              key=lambda x: '_' if 'Value' in x
                                              else x['Direction'])])
            usage = 'Usage: az ml service run batch -n {} {} [-w] [-j <job_id>] [-v]'.format(
                service_name,
                param_str)
            print(usage)

    except requests.ConnectionError:
        print(
        "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return


def batch_view_job(service_name, job_name, verb, context=cli_context):
    """
    Processing for viewing a job on an existing batch service
    :param context: CommandLineInterfaceContext object
    :param verb: bool flag to indicate verbosity
    :param service_name: str name of service
    :param job_name: str name of the job to view
    :return: None
    """
    if not batch_env_is_valid(context):
        return

    success, content = get_success_and_resp_str(context, batch_get_job(context, job_name,
                                                                       service_name,
                                                                       verb),
                                                verbose=verb)
    if success:
        print(batch_get_job_description(context, content))
    else:
        print(content)


def batch_list_jobs(service_name, context=cli_context):
    """
    Processing for listing all jobs of an existing batch service
    :param context: CommandLineInterfaceContext object
    :param service_name: str name of service
    :return: None
    """
    if not batch_env_is_valid(context):
        return

    url = batch_get_url(context, BATCH_ALL_JOBS_FMT, service_name)
    try:
        resp = context.http_call('get', url, auth=(context.hdi_user, context.hdi_pw))
        print(get_success_and_resp_str(context, resp, response_obj=TableResponse(
            batch_list_jobs_header_to_fn_dict))[1])
    except requests.ConnectionError:
        print(
        "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))


def batch_cancel_job(service_name, job_name, verb, context=cli_context):
    """
    Processing for canceling a job on an existing batch service
    :param context: CommandLineInterfaceContext object
    :param verb: bool flag to indicate verbosity
    :param service_name: str name of service
    :param job_name: str name of the job to cancel
    :return: None
    """
    if not batch_env_is_valid(context):
        return

    url = batch_get_url(context, BATCH_CANCEL_JOB_FMT, service_name, job_name)
    if verb:
        print("Canceling job by posting to {}".format(url))
    try:
        resp = context.http_call('post', url,
                                 auth=(context.hdi_user, context.hdi_pw))
        print(
            get_success_and_resp_str(context, resp, response_obj=StaticStringResponse(
                'Job {0} of service {1} canceled.'.format(job_name, service_name)),
                                     verbose=verb)[1])
    except requests.ConnectionError:
        print(
            "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(
                url))


def batch_service_delete(service_name, verb, context=cli_context):
    """
    Processing for deleting a job on an existing batch service
    :param context: CommandLineInterfaceContext object
    :param verb: bool flag to indicate verbosity
    :param service_name: str name of service
    :return: None
    """
    if not batch_env_and_storage_are_valid(context):
        return

    url = batch_get_url(context, BATCH_SINGLE_WS_FMT, service_name)

    try:
        resp = context.http_call('get', url, auth=(context.hdi_user, context.hdi_pw))
    except requests.ConnectionError:
        print(
        "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return

    exists, err_msg = get_success_and_resp_str(context, resp, verbose=verb)
    if not exists:
        print(err_msg)
        return

    if verb:
        print('Deleting resource at {}'.format(url))
    try:
        resp = context.http_call('delete', url, auth=(context.hdi_user, context.hdi_pw))
    except requests.ConnectionError:
        print(
        "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return
    print(get_success_and_resp_str(context, resp, response_obj=StaticStringResponse(
        'Service {} deleted.'.format(service_name)), verbose=verb)[1])


def batch_service_run(service_name, verb, inputs,
                      outputs, parameters, job_name,
                      wait_for_completion,
                      context=cli_context):
    """

    Processing for creating a new batch service
    :param parameters: list of str of format <param_name>[:<default_value>]. value inputs.
    :param outputs: list of str of format <param_name>[:<default_value>]. ref outputs.
    :param inputs: list of str of format <param_name>[:<default_value>]. ref inputs.
    :param verb: bool flag to indicate verbosity
    :param service_name: str name of service
    :param context: CommandLineInterfaceContext object
    :param job_name: str name of job. defaults to time.strftime('%Y-%m-%d_%H%M%S')
    :param wait_for_completion: bool flag to wait, defaults to False
    :return: None
    """
    if not job_name:
        job_name = time.strftime('%Y-%m-%d_%H%M%S')

    input_output_dict = {}
    parameter_dict = {}
    valid_parameters = True
    for param in inputs:
        is_valid, k, v = validate_and_split_run_param(param)
        valid_parameters = valid_parameters and is_valid
        if valid_parameters:
            input_output_dict[k] = v, True

    for param in outputs:
        is_valid, k, v = validate_and_split_run_param(param)
        valid_parameters = valid_parameters and is_valid
        if valid_parameters:
            input_output_dict[k] = v, False

    for param in parameters:
        is_valid, k, v = validate_and_split_run_param(param)
        valid_parameters = valid_parameters and is_valid
        if valid_parameters:
            parameter_dict[k] = v

    if not valid_parameters:
        return

    parameters_container = 'parameters{}'.format(uuid.uuid4())

    # validate environment
    if not batch_env_and_storage_are_valid(context):
        return

    # upload any local parameters if needed
    try:
        parameter_dict.update({param_name: update_asset_path(context, verb,
                                                             input_output_dict[param_name][0],
                                                             parameters_container,
                                                             is_input=
                                                             input_output_dict[param_name][1])[1]
                               for param_name in input_output_dict})
    except ValueError as exc:
        print('Unable to process parameters: {}'.format(exc))
        return

    if verb:
        print('parameters: {0}'.format(parameter_dict))

    arg_payload = {'Parameters': parameter_dict}

    url = batch_get_url(context, BATCH_SINGLE_JOB_FMT, service_name, job_name)
    if verb:
        print('scoring at {}'.format(url))
    headers = {'Content-Type': 'application/json'}
    try:
        resp = context.http_call('put', url,
                                 headers=headers,
                                 data=json.dumps(arg_payload),
                                 auth=(context.hdi_user, context.hdi_pw))
    except requests.ConnectionError:
        print(
            "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(
                url))
        return

    succeeded, to_print = get_success_and_resp_str(context, resp,
                                                   response_obj=StaticStringResponse(
                                                       'Job {0} submitted on service {1}.'.format(
                                                           job_name, service_name)),
                                                   verbose=verb)
    print(to_print)
    if not succeeded:
        return

    if wait_for_completion:
        while True:
            succeeded, to_print = get_success_and_resp_str(context,
                                                           batch_get_job(context,
                                                                         job_name,
                                                                         service_name,
                                                                         verb),
                                                           verbose=verb)
            if not succeeded:
                print(to_print)
                return
            resp_obj = get_json(to_print)
            job_state = resp_obj['State']
            if 'YarnAppId' in resp_obj:
                print('{}: {}'.format(resp_obj['YarnAppId'], job_state))
            else:
                print(job_state)
            if job_state == 'NotStarted' or job_state == 'InProgress':
                time.sleep(5)
            else:
                print(batch_get_job_description(context, to_print))
                break
    else:
        print(
            'To check job status, run: az ml service viewjob batch -n {} -j {}'.format(
                service_name, job_name))


def batch_service_create(driver_file, service_name, title, verb, inputs,
                         outputs, parameters, dependencies,
                         context=cli_context):
    """
    Processing for creating a new batch service
    :param dependencies: list of str named dependencies
    :param parameters: list of str of format <param_name>[:<default_value>]. value inputs.
    :param outputs: list of str of format <param_name>[:<default_value>]. ref outputs.
    :param inputs: list of str of format <param_name>[:<default_value>]. ref inputs.
    :param verb: bool flag to indicate verbosity
    :param title: str title of service, defaults to service_name
    :param service_name: str name of service
    :param driver_file: str path to driver file
    :param context: CommandLineInterfaceContext object
    :return: None
    """

    if verb:
        print('outputs: {0}'.format(outputs))
        print('inputs: {0}'.format(inputs))
        print('parameters: {0}'.format(parameters))
        print('driver_file: {}'.format(driver_file))

    inputs = [(arg, 'Input', 'Reference') for arg in inputs]
    outputs = [(arg, 'Output', 'Reference') for arg in outputs]
    parameters = [(arg, 'Input', 'Value') for arg in parameters]

    if not batch_env_and_storage_are_valid(context):
        return

    if not title:
        title = service_name

    # DEPENDENCIES
    dependency_container = 'dependencies/{}'.format(uuid.uuid4())
    try:
        dependencies = [update_asset_path(context, verb, dependency, dependency_container)
                        for dependency in dependencies]
    except ValueError as exc:
        print('Error uploading dependencies: {}'.format(exc))
        return

    # DRIVER
    try:
        driver_id, driver_uri = update_asset_path(context, verb, driver_file,
                                                  dependency_container)
    except ValueError as exc:
        print('Error uploading driver: {}'.format(exc))
        return

    # modify json payload to update driver package location
    payload = resource_string(__name__, 'data/batch_create_payload.json')
    json_payload = get_json(payload)

    json_payload['Assets'] = [{'Id': driver_id, 'Uri': driver_uri}]
    json_payload['Package']['DriverProgramAsset'] = driver_id

    # OTHER DEPENDENCIES
    for dependency in dependencies:
        json_payload['Assets'].append({'Id': dependency[0], 'Uri': dependency[1]})
        json_payload['Package'][batch_get_asset_type(dependency[0])].append(
            dependency[0])

    # replace inputs from template
    json_payload['Parameters'] = batch_create_parameter_list(
        inputs + outputs + parameters)

    # update assets payload for default inputs
    for parameter in json_payload['Parameters']:
        if 'Value' in parameter:
            if parameter['Kind'] == 'Reference':
                try:
                    asset_id, location = update_asset_path(context, verb,
                                                           parameter['Value'],
                                                           dependency_container,
                                                           parameter['Direction'] ==
                                                           'Input')
                    json_payload['Assets'].append({'Id': asset_id, 'Uri': location})
                    parameter['Value'] = asset_id
                except ValueError as exc:
                    print('Error creating parameter list: {}'.format(exc))
                    return

    # update title
    json_payload['Title'] = title

    if verb:
        print('json_payload: {}'.format(json_payload))

    # call SparkBatch with payload to create web service
    url = batch_get_url(context, BATCH_SINGLE_WS_FMT, service_name)

    if verb:
        print("Creating web service at " + url)

    headers = {'Content-Type': 'application/json'}

    try:
        resp = context.http_call('put', url, headers=headers,
                                 data=json.dumps(json_payload),
                                 auth=(context.hdi_user, context.hdi_pw))
    except requests.ConnectionError:
        print(
        "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return

    # Create usage str: inputs/parameters before ouputs, optional after all
    param_str = ' '.join([batch_get_parameter_str(p) for
                          p in sorted(json_payload['Parameters'],
                                      key=lambda x: '_' if 'Value' in x
                                      else x['Direction'])])

    usage = 'Usage: az ml service run batch -n {} {} [-w] [-j <job_id>] [-v]'.format(
        service_name,
        param_str)

    success, response = get_success_and_resp_str(context, resp,
                                                 response_obj=StaticStringWithTableReponse(
                                                     usage,
                                                     batch_create_service_header_to_fn_dict),
                                                 verbose=verb)
    if success:
        print('Success.')

    print(response)
