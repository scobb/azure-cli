# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
Realtime services functions.

"""

from __future__ import print_function
from builtins import input #pylint: disable=redefined-builtin
import getopt
import json
import os
import os.path
import sys
import time
import uuid
import re
import yaml
import tempfile
from datetime import datetime, timedelta
import subprocess
from pkg_resources import resource_filename
from pkg_resources import resource_string
import requests
import tabulate

from azure.storage.blob import (BlockBlobService, ContentSettings, BlobPermissions)

from .._util import cli_context
from .._util import get_json
from .._util import is_int
from .._util import ice_connection_timeout

from ._docker_utils import check_docker_credentials

from ._realtimeutilities import RealtimeConstants
from ._realtimeutilities import resolve_marathon_base_url
from ._realtimeutilities import get_sample_data
from ._realtimeutilities import try_add_sample_file
from ._realtimeutilities import upload_dependency
from ._realtimeutilities import get_k8s_frontend_url
from ._realtimeutilities import test_acs_k8s
from .._k8s_util import KubernetesOperations
from .._k8s_util import check_for_kubectl
from kubernetes.client.rest import ApiException
from ...ml import __version__


# Local mode functions


def realtime_service_delete_local(service_name, verbose):
    """Delete a locally published realtime web service."""

    try:
        dockerps_output = subprocess.check_output(
            ["docker", "ps", "--filter", "\"label=amlid={}\""
             .format(service_name)]).decode('ascii').rstrip().split("\n")[1:]
    except subprocess.CalledProcessError:
        print('[Local mode] Error retrieving running containers. Please ensure you have permissions to run docker.')
        return

    if dockerps_output is None or len(dockerps_output) == 0:
        print("[Local mode] Error: no service named {} running locally.".format(service_name))
        print("[Local mode] To delete a cluster based service, switch to remote mode first: az ml env remote")
        return

    if len(dockerps_output) != 1:
        print("[Local mode] Error: ambiguous reference - too many containers ({}) with the same label.".format(
            len(dockerps_output)))
        return

    container_id = dockerps_output[0][0:12]
    if verbose:
        print("Killing docker container id {}".format(container_id))

    try:
        di_config = subprocess.check_output(
            ["docker", "inspect", "--format='{{json .Config}}'", container_id]).decode('ascii')
        subprocess.check_call(["docker", "kill", container_id])
        subprocess.check_call(["docker", "rm", container_id])
    except subprocess.CalledProcessError:
        print('[Local mode] Error deleting service. Please ensure you have permissions to run docker.')
        return

    try:
        config = json.loads(di_config)
    except ValueError:
        print('[Local mode] Error removing docker image. Please ensure you have permissions to run docker.')
        return

    if 'Image' in config:
        if verbose:
            print('[Debug] Removing docker image {}'.format(config['Image']))
        try:
            subprocess.check_call(["docker", "rmi", "{}".format(config['Image'])])
        except subprocess.CalledProcessError:
            print('[Local mode] Error removing docker image. Please ensure you have permissions to run docker.')
            return

    print("Service deleted.")
    return


def get_local_realtime_service_port(service_name, verbose):
    """Find the host port mapping for a locally published realtime web service."""

    try:
        dockerps_output = subprocess.check_output(
            ["docker", "ps", "--filter", "\"label=amlid={}\"".format(service_name)]).decode('ascii').rstrip().split("\n") #pylint: disable=line-too-long
    except subprocess.CalledProcessError:
        return -1
    if verbose:
        print("docker ps:")
        print(dockerps_output)
    if len(dockerps_output) == 1:
        return -1
    elif len(dockerps_output) == 2:
        container_id = dockerps_output[1][0:12]
        container_ports = subprocess.check_output(["docker", "port", container_id]).decode('ascii').strip().split('\n')
        container_ports_dict = dict(map((lambda x: tuple(filter(None, x.split('->')))), container_ports))
        # 5001 is the port we expect an ICE-built container to be listening on
        matching_ports = list(filter(lambda k: '5001' in k, container_ports_dict.keys()))
        if matching_ports is None or len(matching_ports) != 1:
            return -2
        container_port = container_ports_dict[matching_ports[0]].split(':')[1].rstrip()
        if verbose:
            print("Container port: {}".format(container_port))
        return container_port
    else:
        return -2


def realtime_service_deploy_local(context, image, verbose, app_insights_enabled, logging_level):
    """Deploy a realtime web service locally as a docker container."""

    print("[Local mode] Running docker container.")
    service_label = image.split("/")[1]

    # Delete any local containers with the same label
    existing_container_port = get_local_realtime_service_port(service_label, verbose)
    if is_int(existing_container_port) and int(existing_container_port) > 0:
        print('Found existing local service with the same name running at http://127.0.0.1:{}/score'
              .format(existing_container_port))
        answer = context.get_input('Delete existing service and create new service (y/N)? ')
        answer = answer.rstrip().lower()
        if answer != 'y' and answer != 'yes':
            print('Canceling service create.')
            return 1
        realtime_service_delete_local(service_label, verbose)

    # Check if credentials to the ACR are already configured in ~/.docker/config.json
    check_docker_credentials(context.acr_home, context.acr_user, context.acr_pw, verbose)

    try:
        subprocess.check_call(['docker', 'pull', image])
        docker_output = subprocess.check_output(
            ["docker", "run", "-e", "AML_APP_INSIGHTS_KEY={}".format(context.app_insights_account_key),
                              "-e", "AML_APP_INSIGHTS_ENABLED={}".format(app_insights_enabled),
                              "-e", "AML_CONSOLE_LOG={}".format(logging_level),
                              "-d", "-P", "-l", "amlid={}".format(service_label), "{}".format(image)]).decode('ascii')
    except subprocess.CalledProcessError:
        print('[Local mode] Error starting docker container. Please ensure you have permissions to run docker.')
        return

    try:
        dockerps_output = subprocess.check_output(["docker", "ps"]).decode('ascii').split("\n")[1:]
    except subprocess.CalledProcessError:
        print('[Local mode] Error starting docker container. Please ensure you have permissions to run docker.')
        return

    container_created = (x for x in dockerps_output if x.startswith(docker_output[0:12])) is not None
    if container_created:
        dockerport = get_local_realtime_service_port(service_label, verbose)
        if int(dockerport) < 0:
            print('[Local mode] Failed to start container. Please report this to deployml@microsoft.com with your image id: {}'.format(image)) #pylint: disable=line-too-long
            return

        sample_data_available = get_sample_data('http://127.0.0.1:{}/sample'.format(dockerport), None, verbose)
        input_data = "'{{\"input\":\"{}\"}}'"\
            .format(sample_data_available if sample_data_available else '!! YOUR DATA HERE !!')
        print("[Local mode] Success.")
        print('[Local mode] Scoring endpoint: http://127.0.0.1:{}/score'.format(dockerport))
        print("[Local mode] Usage: az ml service run realtime -n " + service_label + " [-d {}]".format(input_data))
        return
    else:
        print("[Local mode] Error creating local web service. Docker failed with:")
        print(docker_output)
        print("[Local mode] Please help us improve the product by mailing the logs to ritbhat@microsoft.com")
        return


def realtime_service_run_local(service_name, input_data, verbose):
    """Run a previously published local realtime web service."""

    container_port = get_local_realtime_service_port(service_name, verbose)
    if is_int(container_port) and int(container_port) < 0:
        print("[Local mode] No service named {} running locally.".format(service_name))
        print("To run a remote service, switch environments using: az ml env remote")
        return
    else:
        headers = {'Content-Type': 'application/json'}
        if input_data == '':
            print("No input data specified. Checking for sample data.")
            sample_url = 'http://127.0.0.1:{}/sample'.format(container_port)
            sample_data = get_sample_data(sample_url, headers, verbose)
            input_data = '{{"input":"{}"}}'.format(sample_data)
            if not sample_data:
                print(
                    "No sample data available. To score with your own data, run: az ml service run realtime -n {} -d <input_data>" #pylint: disable=line-too-long
                    .format(service_name))
                return
            print('Using sample data: ' + input_data)
        else:
            if verbose:
                print('[Debug] Input data is {}'.format(input_data))
                print('[Debug] Input data type is {}'.format(type(input_data)))
            try:
                json.loads(input_data)
            except ValueError:
                print('[Local mode] Invalid input. Expected data of the form \'{{"input":"[[val1,val2,...]]"}}\'')
                print('[Local mode] If running from a shell, ensure quotes are properly escaped.')
                return

        service_url = 'http://127.0.0.1:{}/score'.format(container_port)
        if verbose:
            print("Service url: {}".format(service_url))
        try:
            result = requests.post(service_url, headers=headers, data=input_data, verify=False)
        except requests.ConnectionError:
            print('[Local mode] Error connecting to container. Please try recreating your local service.')
            return

        if verbose:
            print(result.content)

        if result.status_code == 200:
            result = result.json()
            print(result['result'])
            return
        else:
            print(result.content)

# Cluster mode functions


def realtime_service_scale(service_name, num_replicas, context=cli_context):
    """Scale a published realtime web service."""

    if context.in_local_mode():
        print("Error: Scaling is not supported in local mode.")
        print("To scale a cluster based service, switch to cluster mode first: az ml env cluster")
        return

    elif context.env_is_k8s:
        try:
            num_replicas = int(num_replicas)
            if num_replicas < 1 or num_replicas > 17:
                raise ValueError
        except ValueError:
            print("The -z option must be an integer in range [1-17] inclusive.")
            return

        ops = KubernetesOperations()
        ops.scale_deployment(service_name, num_replicas)
        return

    else:
        print("Scaling is not currently supported for Mesos clusters.")
        return

    service_name = ''
    instance_count = 0

    try:
        opts, args = getopt.getopt(args, "n:c:")
    except getopt.GetoptError:
        print("az ml service scale realtime -n <service name> -c <instance_count>")
        return

    for opt, arg in opts:
        if opt == '-n':
            service_name = arg
        elif opt == '-c':
            instance_count = int(arg)

    if service_name == '':
        print("Error: missing service name.")
        print("az ml service scale realtime -n <service name> -c <instance_count>")
        return

    if instance_count == 0 or instance_count > 5:
        print("Error: instance count must be between 1 and 5.")
        print("To delete a service, use: az ml service delete")
        return

    headers = {'Content-Type': 'application/json'}
    data = {'instances': instance_count}
    marathon_base_url = resolve_marathon_base_url(context)
    if marathon_base_url is None:
        return
    marathon_url = marathon_base_url + '/marathon/v2/apps'
    success = False
    tries = 0
    print("Scaling service.", end="")
    start = time.time()
    scale_result = requests.put(marathon_url + '/' + service_name, headers=headers, data=json.dumps(data), verify=False)
    if scale_result.status_code != 200:
        print('Error scaling application.')
        print(scale_result.content)
        return

    try:
        scale_result = scale_result.json()
    except ValueError:
        print('Error scaling application.')
        print(scale_result.content)
        return

    if 'deploymentId' in scale_result:
        print("Deployment id: " + scale_result['deploymentId'])
    else:
        print('Error scaling application.')
        print(scale_result.content)
        return

    m_app = requests.get(marathon_url + '/' + service_name)
    m_app = m_app.json()
    while 'deployments' in m_app['app']:
        if not m_app['app']['deployments']:
            success = True
            break
        if int(time.time() - start) > 60:
            break
        tries += 1
        if tries % 5 == 0:
            print(".", end="")
        m_app = requests.get(marathon_url + '/' + service_name)
        m_app = m_app.json()

    print("")

    if not success:
        print("  giving up.")
        print("There may not be enough capacity in the cluster. Please try deleting or scaling down other services first.") #pylint: disable=line-too-long
        return

    print("Successfully scaled service to {} instances.".format(instance_count))
    return


def realtime_service_delete_kubernetes(context, service_name, verbose):
    response = input("Permanently delete service {} (y/N)? ".format(service_name))
    response = response.rstrip().lower()
    if response != 'y' and response != 'yes':
        return

    k8s_ops = KubernetesOperations()
    try:
        if not check_for_kubectl(context):
            print('')
            print('kubectl is required to delete webservices. Please install it on your path and try again.')
            return
        k8s_ops.delete_service(service_name)
        k8s_ops.delete_deployment(service_name)
    except ApiException as exc:
        if exc.status == 404:
            print("Unable to find web service with name {}.".format(service_name))
            return
        print("Exception occurred while trying to delete service {}. {}".format(service_name, exc))


def realtime_service_delete(service_name, verb, context=cli_context):
    """Delete a realtime web service."""

    verbose = verb

    if context.in_local_mode():
        realtime_service_delete_local(service_name, verbose)
        return

    if context.env_is_k8s:
        realtime_service_delete_kubernetes(context, service_name, verbose)
        return

    if context.acs_master_url is None:
        print("")
        print("Please set up your ACS cluster for AML. See 'az ml env about' for more information.")
        return

    response = input("Permanently delete service {} (y/N)? ".format(service_name))
    response = response.rstrip().lower()
    if response != 'y' and response != 'yes':
        return

    headers = {'Content-Type': 'application/json'}
    marathon_base_url = resolve_marathon_base_url(context)
    marathon_url = marathon_base_url + '/marathon/v2/apps'
    try:
        delete_result = requests.delete(marathon_url + '/' + service_name, headers=headers, verify=False)
    except requests.ConnectTimeout:
        print('Error: timed out trying to establish a connection to ACS. Please check that your ACS is up and healthy.')
        print('For more information about setting up your environment, see: "az ml env about".')
        return
    except requests.ConnectionError:
        print('Error: Could not establish a connection to ACS. Please check that your ACS is up and healthy.')
        print('For more information about setting up your environment, see: "az ml env about".')
        return

    if delete_result.status_code != 200:
        print('Error deleting service: {}'.format(delete_result.content))
        return

    try:
        delete_result = delete_result.json()
    except ValueError:
        print('Error deleting service: {}'.format(delete_result.content))
        return

    if 'deploymentId' not in delete_result:
        print('Error deleting service: {}'.format(delete_result.content))
        return

    print("Deployment id: " + delete_result['deploymentId'])
    m_app = requests.get(marathon_url + '/' + service_name)
    m_app = m_app.json()
    transient_error_count = 0
    while ('app' in m_app) and ('deployments' in m_app['app']):
        if not m_app['app']['deployments']:
            break
        try:
            m_app = requests.get(marathon_url + '/' + service_name)
        except (requests.ConnectionError, requests.ConnectTimeout):
            if transient_error_count < 3:
                print('Error: lost connection to ACS cluster. Retrying...')
                continue
            else:
                print('Error: too many retries. Giving up.')
                return
        m_app = m_app.json()

    print("Deleted.")
    return


def realtime_service_create(score_file, dependencies, requirements, schema_file, service_name,
                            verb, custom_ice_url, target_runtime, logging_level, model, num_replicas,
                            context=cli_context):
    """Create a new realtime web service."""

    verbose = verb
    if logging_level == 'none':
        app_insights_enabled = 'false'
        logging_level = 'debug'
    else:
        app_insights_enabled = 'true'

    is_known_runtime = \
        target_runtime in RealtimeConstants.supported_runtimes or target_runtime in RealtimeConstants.ninja_runtimes
    if score_file == '' or service_name == '' or not is_known_runtime:
        print(RealtimeConstants.create_cmd_sample)
        return

    storage_exists = False
    acr_exists = False

    if context.az_account_name is None or context.az_account_key is None:
        print("")
        print("Please set up your storage account for AML:")
        print("  export AML_STORAGE_ACCT_NAME=<yourstorageaccountname>")
        print("  export AML_STORAGE_ACCT_KEY=<yourstorageaccountkey>")
        print("")
    else:
        storage_exists = True

    if context.in_local_mode():
        acs_exists = True
    elif context.env_is_k8s:
        acs_exists = test_acs_k8s()
        if not acs_exists:
            print('')
            print('Your Kubernetes cluster is not responding as expected.')
            print('Please verify it is healthy. If you set it up via `az ml env setup,` '
                  'please contact deployml@microsoft.com to troubleshoot.')
            print('')
    else:
        acs_exists = context.acs_master_url and context.acs_agent_url
        if not acs_exists:
            print("")
            print("Please set up your ACS cluster for AML:")
            print("  export AML_ACS_MASTER=<youracsmasterdomain>")
            print("  export AML_ACS_AGENT=<youracsagentdomain>")
            print("")

    if context.acr_home is None or context.acr_user is None or context.acr_pw is None:
        print("")
        print("Please set up your ACR registry for AML:")
        print("  export AML_ACR_HOME=<youracrdomain>")
        print("  export AML_ACR_USER=<youracrusername>")
        print("  export AML_ACR_PW=<youracrpassword>")
        print("")
    else:
        acr_exists = True

    if context.env_is_k8s and not re.match(r"[a-zA-Z0-9\.-]+", service_name):
        print("Kubernetes Service names may only contain alphanumeric characters, '.', and '-'")
        return

    if not storage_exists or not acs_exists or not acr_exists:
        return

    # modify json payload to update assets and driver location
    payload = resource_string(__name__, 'data/testrequest.json')
    json_payload = json.loads(payload.decode('ascii'))

    # update target runtime in payload
    json_payload['properties']['deploymentPackage']['targetRuntime'] = target_runtime

    # upload target storage for resources
    json_payload['properties']['storageAccount']['name'] = context.az_account_name
    json_payload['properties']['storageAccount']['key'] = context.az_account_key

    # Add dependencies

    # If there's a model specified, add it as a dependency
    if model:
        dependencies.append(model)

    # Always inject azuremlutilities.py as a dependency from the CLI
    # It contains helper methods for serializing and deserializing schema
    utilities_filename = resource_filename(__name__, 'azuremlutilities.py')
    dependencies.append(utilities_filename)

    # If a schema file was provided, try to find the accompanying sample file
    # and add as a dependency
    get_sample_code = ''
    if schema_file is not '':
        dependencies.append(schema_file)
        sample_added, sample_filename = try_add_sample_file(dependencies, schema_file, verbose)
        if sample_added:
            get_sample_code = \
                resource_string(__name__, 'data/getsample.py').decode('ascii').replace('PLACEHOLDER', sample_filename)

    if requirements is not '':
        if verbose:
            print('Uploading requirements file: {}'.format(requirements))
            (status, location, filename) = \
                upload_dependency(context, requirements, verbose)
            if status < 0:
                print('Error resolving requirements file: no such file or directory {}'.format(requirements))
                return
            else:
                json_payload['properties']['deploymentPackage']['pipRequirements'] = location

    dependency_injection_code = '\nimport tarfile\nimport os.path\n'
    dependency_count = 0
    if dependencies is not None:
        print('Uploading dependencies.')
        for dependency in dependencies:
            (status, location, filename) = \
                upload_dependency(context, dependency, verbose)
            if status < 0:
                print('Error resolving dependency: no such file or directory {}'.format(dependency))
                return
            else:
                dependency_count += 1
                # Add the new asset to the payload
                new_asset = {'mimeType': 'application/octet-stream',
                             'id': str(dependency),
                             'location': location}
                json_payload['properties']['assets'].append(new_asset)
                if verbose:
                    print("Added dependency {} to assets.".format(dependency))

                # If the asset was a directory, also add code to unzip and layout directory
                if status == 1:
                    dependency_injection_code = dependency_injection_code + \
                                                'if os.path.exists("{}"):\n'.format(filename) + \
                                                '  amlbdws_dependency_{} = tarfile.open("{}")\n'\
                                                .format(dependency_count, filename)
                    dependency_injection_code = dependency_injection_code + \
                                                '  amlbdws_dependency_{}.extractall()\n'.format(dependency_count)

    if verbose:
        print("Code injected to unzip directories:\n{}".format(dependency_injection_code))
        print(json.dumps(json_payload))

    # read in code file
    if os.path.isfile(score_file):
        with open(score_file, 'r') as scorefile:
            code = scorefile.read()
    else:
        print("Error: No such file {}".format(score_file))
        return

    if target_runtime == 'spark-py':
        # read in fixed preamble code
        preamble = resource_string(__name__, 'data/preamble').decode('ascii')

        # wasb configuration: add the configured storage account in the as a wasb location
        wasb_config = "spark.sparkContext._jsc.hadoopConfiguration().set('fs.azure.account.key." + \
                      context.az_account_name + ".blob.core.windows.net','" + context.az_account_key + "')"

        # create blob with preamble code and user function definitions from cell
        code = "{}\n{}\n{}\n{}\n\n\n{}".format(preamble, wasb_config, dependency_injection_code, code, get_sample_code)
    else:
        code = "{}\n{}\n\n\n{}".format(dependency_injection_code, code, get_sample_code)

    if verbose:
        print(code)

    az_container_name = 'amlbdpackages'
    az_blob_name = str(uuid.uuid4()) + '.py'
    bbs = BlockBlobService(account_name=context.az_account_name,
                           account_key=context.az_account_key)
    bbs.create_container(az_container_name)
    bbs.create_blob_from_text(az_container_name, az_blob_name, code,
                              content_settings=ContentSettings(content_type='application/text'))
    blob_sas = bbs.generate_blob_shared_access_signature(
        az_container_name,
        az_blob_name,
        BlobPermissions.READ,
        datetime.utcnow() + timedelta(days=30))
    package_location = 'http://{}.blob.core.windows.net/{}/{}?{}'.format(context.az_account_name,
                                                                         az_container_name, az_blob_name, blob_sas)

    if verbose:
        print("Package uploaded to " + package_location)

    for asset in json_payload['properties']['assets']:
        if asset['id'] == 'driver_package_asset':
            if verbose:
                print("Current driver location:", str(asset['location']))
                print("Replacing with:", package_location)
            asset['location'] = package_location

    # modify json payload to set ACR credentials
    if verbose:
        print("Current ACR creds in payload:")
        print('location:', json_payload['properties']['registryInfo']['location'])
        print('user:', json_payload['properties']['registryInfo']['user'])
        print('password:', json_payload['properties']['registryInfo']['password'])

    json_payload['properties']['registryInfo']['location'] = context.acr_home
    json_payload['properties']['registryInfo']['user'] = context.acr_user
    json_payload['properties']['registryInfo']['password'] = context.acr_pw

    if verbose:
        print("New ACR creds in payload:")
        print('location:', json_payload['properties']['registryInfo']['location'])
        print('user:', json_payload['properties']['registryInfo']['user'])
        print('password:', json_payload['properties']['registryInfo']['password'])

    # call ICE with payload to create docker image

    # Set base ICE URL
    if custom_ice_url is not '':
        base_ice_url = custom_ice_url
        if base_ice_url.endswith('/'):
            base_ice_url = base_ice_url[:-1]
    else:
        base_ice_url = 'https://amlacsagent.azureml-int.net'

    create_url = base_ice_url + '/images/' + service_name
    get_url = base_ice_url + '/jobs'
    headers = {'Content-Type': 'application/json', 'User-Agent': 'aml-cli-{}'.format(__version__)}

    image = ''
    max_retries = 3
    try_number = 0
    ice_put_result = {}
    while try_number < max_retries:
        try:
            ice_put_result = requests.put(
                create_url, headers=headers, data=json.dumps(json_payload), timeout=ice_connection_timeout)
            break
        except (requests.ConnectionError, requests.exceptions.ReadTimeout):
            if try_number < max_retries:
                try_number += 1
                continue
            print('Error: could not connect to Azure ML. Please try again later. If the problem persists, please contact deployml@microsoft.com') #pylint: disable=line-too-long
            return

    if ice_put_result.status_code == 401:
        print("Invalid API key. Please update your key by running 'az ml env key -u'.")
        return
    elif ice_put_result.status_code != 201:
        print('Error connecting to Azure ML. Please contact deployml@microsoft.com with the stack below.')
        print(ice_put_result.content)
        return

    if verbose:
        print(ice_put_result)
    if isinstance(ice_put_result.json(), str):
        return json.dumps(ice_put_result.json())

    job_id = ice_put_result.json()['Job Id']
    if verbose:
        print('ICE URL: ' + create_url)
        print('Submitted job with id: ' + json.dumps(job_id))
    else:
        sys.stdout.write('Creating docker image.')
        sys.stdout.flush()

    job_status = requests.get(get_url + '/' + job_id, headers=headers)
    response_payload = job_status.json()
    while 'Provisioning State' in response_payload:
        job_status = requests.get(get_url + '/' + job_id, headers=headers)
        response_payload = job_status.json()
        if response_payload['Provisioning State'] == 'Running':
            time.sleep(5)
            if verbose:
                print("Provisioning image. Details: " + response_payload['Details'])
            else:
                sys.stdout.write('.')
                sys.stdout.flush()
            continue
        else:
            if response_payload['Provisioning State'] == 'Succeeded':
                acs_payload = response_payload['ACS_PayLoad']
                acs_payload['container']['docker']['image'] = json_payload['properties']['registryInfo']['location'] \
                                                              + '/' + service_name
                image = acs_payload['container']['docker']['image']
                break
            else:
                print('Error creating image: ' + json.dumps(response_payload))
                return

    print('done.')
    print('Image available at : {}'.format(acs_payload['container']['docker']['image']))
    if context.in_local_mode():
        return realtime_service_deploy_local(context, image, verbose, app_insights_enabled, logging_level)
    elif context.env_is_k8s:
        realtime_service_deploy_k8s(context, image, service_name, app_insights_enabled, logging_level, num_replicas)
    else:
        realtime_service_deploy(context, image, service_name, app_insights_enabled, logging_level, verbose)


def realtime_service_deploy(context, image, app_id, app_insights_enabled, logging_level, verbose):
    """Deploy a realtime web service from a docker image."""

    marathon_app = resource_string(__name__, 'data/marathon.json')
    marathon_app = json.loads(marathon_app.decode('ascii'))
    marathon_app['container']['docker']['image'] = image
    marathon_app['labels']['HAPROXY_0_VHOST'] = context.acs_agent_url
    marathon_app['labels']['AMLID'] = app_id
    marathon_app['env']['AML_APP_INSIGHTS_KEY'] = context.app_insights_account_key
    marathon_app['env']['AML_APP_INSIGHTS_ENABLED'] = app_insights_enabled
    marathon_app['env']['AML_CONSOLE_LOG'] = logging_level
    marathon_app['id'] = app_id

    if verbose:
        print('Marathon payload: {}'.format(marathon_app))

    headers = {'Content-Type': 'application/json'}
    marathon_base_url = resolve_marathon_base_url(context)
    marathon_url = marathon_base_url + '/marathon/v2/apps'
    try:
        deploy_result = requests.put(
            marathon_url + '/' + app_id, headers=headers, data=json.dumps(marathon_app), verify=False)
    except requests.exceptions.ConnectTimeout:
        print('Error: timed out trying to establish a connection to ACS. Please check that your ACS is up and healthy.')
        print('For more information about setting up your environment, see: "az ml env about".')
        return
    except requests.ConnectionError:
        print('Error: Could not establish a connection to ACS. Please check that your ACS is up and healthy.')
        print('For more information about setting up your environment, see: "az ml env about".')
        return

    try:
        deploy_result.raise_for_status()
    except requests.exceptions.HTTPError as ex:
        print('Error creating service: {}'.format(ex))
        return

    try:
        deploy_result = get_json(deploy_result.content)
    except ValueError:
        print('Error creating service.')
        print(deploy_result.content)
        return

    print("Deployment id: " + deploy_result['deploymentId'])
    m_app = requests.get(marathon_url + '/' + app_id)
    m_app = m_app.json()
    while 'deployments' in m_app['app']:
        if not m_app['app']['deployments']:
            break
        m_app = requests.get(marathon_url + '/' + app_id)
        m_app = m_app.json()

    print("Success.")
    print("Usage: az ml service run realtime -n " + app_id + " [-d '{\"input\" : \"!! YOUR DATA HERE !!\"}']")


def realtime_service_deploy_k8s(context, image, app_id, app_insights_enabled, logging_level, num_replicas):
    """Deploy a realtime Kubernetes web service from a docker image."""

    k8s_template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     'data', 'kubernetes_deployment_template.yaml')
    k8s_service_template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                             'data', 'kubernetes_service_template.yaml')
    k8s_fd, tmp_k8s_path = tempfile.mkstemp()
    num_replicas = int(num_replicas)

    try:
        with open(k8s_template_path) as f:
            kubernetes_app = yaml.load(f)
    except OSError as exc:
        print("Unable to find kubernetes deployment template file.".format(exc))
        raise
    kubernetes_app['metadata']['name'] = app_id + '-deployment'
    kubernetes_app['spec']['replicas'] = num_replicas
    kubernetes_app['spec']['template']['spec']['containers'][0]['image'] = image
    kubernetes_app['spec']['template']['spec']['containers'][0]['name'] = app_id
    kubernetes_app['spec']['template']['metadata']['labels']['webservicename'] = app_id
    kubernetes_app['spec']['template']['metadata']['labels']['azuremlappname'] = app_id
    kubernetes_app['spec']['template']['metadata']['labels']['type'] = "realtime"
    kubernetes_app['spec']['template']['spec']['containers'][0]['env'][0]['value'] = context.app_insights_account_key
    kubernetes_app['spec']['template']['spec']['containers'][0]['env'][1]['value'] = app_insights_enabled
    kubernetes_app['spec']['template']['spec']['containers'][0]['env'][2]['value'] = logging_level
    kubernetes_app['spec']['template']['spec']['imagePullSecrets'][0]['name'] = context.acr_user + 'acrkey'

    with open(tmp_k8s_path, 'w') as f:
        yaml.dump(kubernetes_app, f, default_flow_style=False)

    k8s_ops = KubernetesOperations()
    timeout_seconds = 1200
    try:
        k8s_ops.deploy_deployment(tmp_k8s_path, timeout_seconds, num_replicas, context.acr_user + 'acrkey')
        k8s_ops.create_service(k8s_service_template_path, app_id, 'realtime')

        print("Success.")
        print("Usage: az ml service run realtime -n " + app_id + " [-d '{\"input\" : \"!! YOUR DATA HERE !!\"}']")
    except ApiException as exc:
        print("An exception occurred while deploying the service. {}".format(exc))
    finally:
        os.close(k8s_fd)
        os.remove(tmp_k8s_path)


def realtime_service_view(service_name=None, verb=False, context=cli_context):
    """View details of a previously published realtime web service."""

    verbose = verb

    # First print the list view of this service
    num_services = _realtime_service_list(service_name, verb, context)

    scoring_url = None
    usage_headers = ['-H "Content-Type:application/json"']
    default_sample_data = '!!!YOUR DATA HERE !!!'

    if context.in_local_mode():
        try:
            dockerps_output = subprocess.check_output(
                ["docker", "ps", "--filter", "\"label=amlid={}\"".format(service_name)])
            dockerps_output = dockerps_output.decode('ascii').rstrip().split("\n")[1:]
        except subprocess.CalledProcessError:
            print('[Local mode] Error retrieving container details. Make sure you can run docker.')
            return

        if not dockerps_output or dockerps_output is None:
            print('No such service {}.'.format(service_name))
            return

        container_id = dockerps_output[0][0:12]
        try:
            di_network = subprocess.check_output(
                ["docker", "inspect", "--format='{{json .NetworkSettings}}'", container_id]).decode('ascii')
        except subprocess.CalledProcessError:
            print('[Local mode] Error inspecting container. Make sure you can run docker.')
            return

        try:
            net_config = json.loads(di_network)
        except ValueError:
            print('[Local mode] Error retrieving container information. Make sure you can run docker.')
            return

        if 'Ports' in net_config:
            # Find the port mapped to 5001, which is where we expect our container to be listening
            scoring_port_key = [x for x in net_config['Ports'].keys() if '5001' in x]
            if len(scoring_port_key) != 1:
                print('[Local mode] Error: Malconfigured container. Cannot determine scoring port.')
                return
            scoring_port_key = scoring_port_key[0]
            scoring_port = net_config['Ports'][scoring_port_key][0]['HostPort']
            if scoring_port:
                scoring_url = 'http://127.0.0.1:' + str(scoring_port) + '/score'

            # Try to get the sample request from the container
            sample_url = 'http://127.0.0.1:' + str(scoring_port) + '/sample'
            headers = {'Content-Type':'application/json'}
        else:
            print('[Local mode] Error: Misconfigured container. Cannot determine scoring port.')
            return
    else:
        if context.env_is_k8s:
            try:
                fe_url = get_k8s_frontend_url()
            except ApiException:
                return
            scoring_url = fe_url + service_name + '/score'
            sample_url = fe_url + service_name + '/sample'
            headers = {'Content-Type': 'application/json'}
        else:
            if context.acs_agent_url is not None:
                scoring_url = 'http://' + context.acs_agent_url + ':9091/score'
                sample_url = 'http://' + context.acs_agent_url + ':9091/sample'
                headers = {'Content-Type': 'application/json', 'X-Marathon-App-Id': "/{}".format(service_name)}
                usage_headers.append('-H "X-Marathon-App-Id:/{}"'.format(service_name))
            else:
                print('Unable to determine ACS Agent URL. '
                      'Please ensure that AML_ACS_AGENT environment variable is set.')
                return

    service_sample_data = get_sample_data(sample_url, headers, verbose)
    sample_data = '{{"input":"{}"}}'.format(
        service_sample_data if service_sample_data is not None else default_sample_data)
    if num_services:
        print('Usage:')
        print('  az ml  : az ml service run realtime -n {} [-d \'{}\']'.format(service_name, sample_data))
        print('  curl : curl -X POST {} --data \'{}\' {}'.format(' '.join(usage_headers), sample_data, scoring_url))


def realtime_service_list(service_name=None, verb=False, context=cli_context):
    _realtime_service_list(service_name, verb, context)


def _realtime_service_list(service_name=None, verb=False, context=cli_context):
    """List published realtime web services."""

    verbose = verb

    if context.in_local_mode():
        if service_name is not None:
            filter_expr = "\"label=amlid={}\"".format(service_name)
        else:
            filter_expr = "\"label=amlid\""

        try:
            dockerps_output = subprocess.check_output(
                ["docker", "ps", "--filter", filter_expr]).decode('ascii').rstrip().split("\n")[1:]
        except subprocess.CalledProcessError:
            print('[Local mode] Error retrieving running containers. Please ensure you have permissions to run docker.')
            return
        if dockerps_output is not None:
            app_table = [['NAME', 'IMAGE', 'CPU', 'MEMORY', 'STATUS', 'INSTANCES', 'HEALTH']]
            for container in dockerps_output:
                container_id = container[0:12]
                try:
                    di_config = subprocess.check_output(
                        ["docker", "inspect", "--format='{{json .Config}}'", container_id]).decode('ascii')
                    di_state = subprocess.check_output(
                        ["docker", "inspect", "--format='{{json .State}}'", container_id]).decode('ascii')
                except subprocess.CalledProcessError:
                    print('[Local mode] Error inspecting docker container. Please ensure you have permissions to run docker.') #pylint: disable=line-too-long
                    if verbose:
                        print('[Debug] Container id: {}'.format(container_id))
                    return
                try:
                    config = json.loads(di_config)
                    state = json.loads(di_state)
                except ValueError:
                    print('[Local mode] Error retrieving container details. Skipping...')
                    return

                # Name of the app
                if 'Labels' in config and 'amlid' in config['Labels']:
                    app_entry = [config['Labels']['amlid']]
                else:
                    app_entry = ['Unknown']

                # Image from the registry
                if 'Image' in config:
                    app_entry.append(config['Image'])
                else:
                    app_entry.append('Unknown')

                # CPU and Memory are currently not reported for local containers
                app_entry.append('N/A')
                app_entry.append('N/A')

                # Status
                if 'Status' in state:
                    app_entry.append(state['Status'])
                else:
                    app_entry.append('Unknown')

                # Instances is always 1 for local containers
                app_entry.append(1)

                # Health is currently not reported for local containers
                app_entry.append('N/A')
                app_table.append(app_entry)
            print(tabulate.tabulate(app_table, headers='firstrow', tablefmt='psql'))

            return len(app_table) - 1

    # Cluster mode
    if context.env_is_k8s:
        return realtime_service_list_kubernetes(context, service_name, verbose)

    if service_name is not None:
        extra_filter_expr = ", AMLID=={}".format(service_name)
    else:
        extra_filter_expr = ""

    marathon_base_url = resolve_marathon_base_url(context)
    if not marathon_base_url:
        return
    marathon_url = marathon_base_url + '/marathon/v2/apps?label=AMLBD_ORIGIN' + extra_filter_expr
    if verbose:
        print(marathon_url)
    try:
        list_result = requests.get(marathon_url)
    except requests.ConnectionError:
        print('Error connecting to ACS. Please check that your ACS cluster is up and healthy.')
        return
    try:
        apps = list_result.json()
    except ValueError:
        print('Error retrieving apps from ACS. Please check that your ACS cluster is up and healthy.')
        print(list_result.content)
        return

    if 'apps' in apps and len(apps['apps']) > 0:
        app_table = [['NAME', 'IMAGE', 'CPU', 'MEMORY', 'STATUS', 'INSTANCES', 'HEALTH']]
        for app in apps['apps']:
            if 'container' in app and 'docker' in app['container'] and 'image' in app['container']['docker']:
                app_image = app['container']['docker']['image']
            else:
                app_image = 'Unknown'
            app_entry = [app['id'].strip('/'), app_image, app['cpus'], app['mem']]
            app_instances = app['instances']
            app_tasks_running = app['tasksRunning']
            app_deployments = app['deployments']
            running = app_tasks_running > 0
            deploying = len(app_deployments) > 0
            suspended = app_instances == 0 and app_tasks_running == 0
            app_status = 'Deploying' if deploying else 'Running' if running else 'Suspended' if suspended else 'Unknown'
            app_entry.append(app_status)
            app_entry.append(app_instances)
            app_healthy_tasks = app['tasksHealthy']
            app_unhealthy_tasks = app['tasksUnhealthy']
            app_health = 'Unhealthy' if app_unhealthy_tasks > 0 else 'Healthy' if app_healthy_tasks > 0 else 'Unknown'
            app_entry.append(app_health)
            app_table.append(app_entry)
        print(tabulate.tabulate(app_table, headers='firstrow', tablefmt='psql'))
        return len(app_table) - 1
    else:
        if service_name:
            print('No service running with name {} on your ACS cluster'.format(service_name))
        else:
            print('No running services on your ACS cluster')


def realtime_service_list_kubernetes(context, service_name=None, verbose=False):
    label_selector = "type==realtime"
    if service_name is not None:
        label_selector += ",webservicename=={}".format(service_name)

    if verbose:
        print("label selector: {}".format(label_selector))

    try:
        k8s_ops = KubernetesOperations()
        list_result = k8s_ops.get_filtered_deployments(label_selector)
    except ApiException as exc:
        print("Failed to list deployments. {}".format(exc))
        return

    if verbose:
        print("Retrieved deployments: ")
        print(list_result)

    if len(list_result) > 0:
        app_table = [['NAME', 'IMAGE', 'STATUS', 'INSTANCES', 'HEALTH']]
        for app in list_result:
            app_image = app.spec.template.spec.containers[0].image
            app_name = app.metadata.labels['webservicename']
            app_status = app.status.conditions[0].type
            app_instances = app.status.replicas
            app_health = 'Healthy' if app.status.unavailable_replicas is None else 'Unhealthy'
            app_entry = [app_name, app_image, app_status, app_instances, app_health]
            app_table.append(app_entry)
        print(tabulate.tabulate(app_table, headers='firstrow', tablefmt='psql'))
        return len(app_table) - 1
    else:
        if service_name:
            print('No service running with name {} on your ACS cluster'.format(service_name))
        else:
            print('No running services on your ACS cluster')


def realtime_service_run_cluster(context, service_name, input_data, verbose):
    """Run a previously published realtime web service in an ACS cluster."""

    if context.acs_agent_url is None:
        print("")
        print("Please set up your ACS cluster for AML. Run 'az ml env about' for help on setting up your environment.")
        print("")
        return

    headers = {'Content-Type': 'application/json', 'X-Marathon-App-Id': "/{}".format(service_name)}

    if input_data == '':
        sample_url = 'http://' + context.acs_agent_url + ':9091/sample'
        sample_data = get_sample_data(sample_url, headers, verbose)

        if sample_data is None:
            print('No such service {}'.format(service_name))
            return
        elif sample_data == '':
            print(
                "No sample data available. To score with your own data, run: az ml service run realtime -n {} -d <input_data>" #pylint: disable=line-too-long
                .format(service_name))
            return

        input_data = '{{"input":"{}"}}'.format(sample_data)
        print('Using sample data: ' + input_data)

    marathon_url = 'http://' + context.acs_agent_url + ':9091/score'
    result = requests.post(marathon_url, headers=headers, data=input_data, verify=False)
    if verbose:
        print(result.content)

    if result.status_code != 200:
        print('Error scoring the service.')
        print(result.content)
        return

    try:
        result = result.json()
    except ValueError:
        print('Error scoring the service.')
        print(result.content)
        return

    print(result['result'])


def realtime_service_run_kubernetes(context, service_name, input_data, verbose):
    ops = KubernetesOperations()
    try:
        ops.get_service(service_name)
    except ApiException:
        print("Unable to find service with name {}".format(service_name))
        return

    headers = {'Content-Type': 'application/json'}
    try:
        frontend_service_url = get_k8s_frontend_url()
    except ApiException as exc:
        print("Unable to connect to Kubernetes Front-End service. {}".format(exc))
        return
    if input_data is None:
        sample_endpoint = frontend_service_url + service_name + '/sample'
        input_data = get_sample_data(sample_endpoint, headers, verbose)

    scoring_endpoint = frontend_service_url + service_name + '/score'
    result = requests.post(scoring_endpoint, data=input_data, headers=headers)
    if verbose:
        print(result.content)

    if not result.ok:
        print('Error scoring the service.')
        content = result.content.decode()
        if content == "ehostunreach":
            print('Unable to reach the requested host.')
            print('If you just created this service, it may not be available yet. Please try again in a few minutes.')
        elif '%MatchError' in content:
            print('Unable to find service with name {}'.format(service_name))
        print(content)
        return

    try:
        result = result.json()
    except ValueError:
        print('Error scoring the service.')
        print(result.content)
        return

    print(result['result'])


def realtime_service_run(service_name, input_data, verb, context=cli_context):
    """
    Execute a previously published realtime web service.
    :param context: CommandLineInterfaceContext object
    :param args: list of str arguments
    """

    verbose = verb

    if verbose:
        print("data: {}".format(input_data))

    if context.in_local_mode():
        realtime_service_run_local(service_name, input_data, verbose)
    elif context.env_is_k8s:
        realtime_service_run_kubernetes(context, service_name, input_data, verbose)
    else:
        realtime_service_run_cluster(context, service_name, input_data, verbose)


