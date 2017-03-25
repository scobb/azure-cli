"""
Utilities for testing batch functionality of amlbdcli
"""
import uuid
import re
import json
from test_util import execute_cmd

successful_publish_regex = r'Usage: aml service run batch -n (?P<service_name>[^ ]*)'
score_sync_regex = r'(Succeeded|Failed)(?P<json_str>{.*})'
score_async_json_regex = r'Job (?P<job_id>[^ ]*) submitted ' \
                         r'on service (?P<service_name>[^\.]*)\.'

remote_train_path = "https://azuremlbatchint.blob.core.windows.net/azureml/" \
                    "jobinputs/food_inspection_trainer/food_inspections1.csv"
remote_test_path = "https://azuremlbatchint.blob.core.windows.net/azureml/jobinputs/" \
                   "food_inspection_trainer/food_inspections2.csv"
remote_eval_output_path_fmt = 'https://azuremlbatchint.blob.core.windows.net/azureml/' \
                              'joboutputs/food_inspection_trainer/eval_results{}.parquet'
remote_model_output_path_fmt = 'https://azuremlbatchint.blob.core.windows.net/' \
                               'azureml/joboutputs/food_inspection_trainer/' \
                               'trained_model{}.model'

existing_batch_service = 'testserviced36755b3-fb86-46dd-801b-084be8dfd5d4'
existing_batch_job = '2017-01-31_001528'

list_service_headers = ['NAME', 'LAST_MODIFIED_AT', 'ENVIRONMENT']
list_job_headers = ['NAME', 'LAST_MODIFIED_AT', 'ENVIRONMENT']
view_service_headers = ['NAME', 'ENVIRONMENT', 'SCORING_URL', 'INPUTS', 'OUTPUTS', 'PARAMETERS']

job_state_regex = r'State: (?P<job_state>.*)'

viewjob_cmd = 'az ml service viewjob batch'
view_service_cmd = 'az ml service view batch'
create_cmd = 'az ml service create batch'
list_cmd = 'az ml service list batch'
listjobs_cmd = 'az ml service listjobs batch'
canceljob_cmd = 'az ml service canceljob batch'
delete_service_cmd = 'az ml service delete batch'
run_service_cmd = 'az ml service run batch'


def param_dict_to_str(param_dict, label):
    return ' '.join(['{0}={1}'.format(label, key) if param_dict[key] is None else
                     '{0}={1}:{2}'.format(label, key, param_dict[key])
                     for key in param_dict])


def get_json_str(std_out):
    if std_out:
        try:
            json_obj = json.loads(std_out)
            return json.dumps(json_obj)
        except Exception as exc:
            print('Exception parsing: {}'.format(exc))
            pass
    return None


def parse_table(std_out):
    if std_out:
        # confirm table format
        if std_out.split('\n')[0].replace('+', '').replace('-', ''):
            return None, None

        headers = []
        num_tables = 0
        lines = std_out.split('\n')
        # parse headers
        for i, line in enumerate(lines):
            if line.startswith('+') and i + 1 < len(lines) and lines[i+1].startswith('|'):
                headers += [header for header in lines[i+1].split() if header != '|']
                num_tables += 1

        return headers, (len(lines) / num_tables) - 4
    return None, None


def batch_view_job(webservice_name, job_name):
    cmd = '{} -n {} -j {}'.format(viewjob_cmd, webservice_name, job_name)
    out, err = execute_cmd(cmd)
    s = re.search(job_state_regex, out)
    if s:
        return s.group('job_state')


def batch_view_service(service_name):
    """

    :param service_name: str service name to view
    :return: str json string, None if failure parsing
    """
    cmd = '{} -n {}'.format(view_service_cmd, service_name)
    out, err = execute_cmd(cmd)
    return parse_table(out)


def batch_publish(driver_path, inputs, outputs, parameters, dependencies=None):
    """

    :param driver_path: string filepath to driver python program
    :param inputs: dict
    :param outputs: dict
    :param parameters: dict
    :param dependencies: list
    :return: str service name (None if not published)
    """
    inputs = param_dict_to_str(inputs, '--in')
    outputs = param_dict_to_str(outputs, '--out')
    parameters = param_dict_to_str(parameters, '--param')
    dependencies = ' '.join(['-d {}'.format(dependency)
                            for dependency in (dependencies if dependencies else [])])
    service_name = 'testservice{}'.format(uuid.uuid4())
    cmd = '{} -f {} {} {} {} {} -n {} -v'.format(create_cmd, driver_path, inputs,
                                                                    outputs, parameters,
                                                                    dependencies,
                                                                    service_name)
    out, err = execute_cmd(cmd)
    s = re.search(successful_publish_regex, out)
    if s:
        return s.group('service_name')
    return None


def batch_list():
    """

    :return: str JSON list of services (None if unable to parse)
    """
    out, err = execute_cmd(list_cmd)
    return parse_table(out)


def batch_list_jobs(service_name):
    """

    :param service_name: name of service to retrieve jobs for
    :return: str JSON list of jobs (None if unable to parse)
    """
    cmd = '{} -n {}'.format(listjobs_cmd, service_name)
    out, err = execute_cmd(cmd)
    return parse_table(out)


def batch_cancel_job(service_name, job_id):
    """

    :param service_name: str name of service
    :param job_id: str name of job
    :return:
    """
    cmd = '{} -n {} -j {}'.format(canceljob_cmd, service_name, job_id)
    out, err = execute_cmd(cmd)
    return out


def batch_delete_service(service_name):
    """

    :param service_name: str name of service
    :return:
    """
    cmd = '{} -n {}'.format(delete_service_cmd, service_name)
    out, err = execute_cmd(cmd)
    return out, err


def batch_score(service_name, inputs, outputs, parameters, wait=True, job_id=None):
    """

    :param service_name: str
    :param inputs: dict
    :param outputs: dict
    :param parameters: dict
    :param wait: bool indicates to wait synchronously for the job
    :param job_id: str name of job
    :return: str wait ? result_json : job_id
    """
    inputs = param_dict_to_str(inputs, '--in')
    outputs = param_dict_to_str(outputs, '--out')
    parameters = param_dict_to_str(parameters, '--param')
    cmd = '{} -n {} {} {} {}'.format(run_service_cmd, service_name, inputs,
                                                        outputs, parameters)
    if wait:
        cmd += ' -w'
    if job_id:
        cmd += ' -j {}'.format(job_id)

    out, err = execute_cmd(cmd)

    if wait:
        s = re.search(job_state_regex, out)
        if s:
            return s.group('job_state')
    else:
        s = re.search(score_async_json_regex, out)
        if s:
            return s.group('job_id')
