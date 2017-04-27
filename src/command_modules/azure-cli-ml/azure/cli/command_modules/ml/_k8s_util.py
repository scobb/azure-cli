from kubernetes import client, config
from kubernetes.client.rest import ApiException
from base64 import b64encode

import yaml
import time
import json
import os
import re
import subprocess
from builtins import input
from ._az_util import az_create_kubernetes
from ._az_util import az_get_k8s_credentials
from ._az_util import az_get_active_email
from ._az_util import az_install_kubectl
from ._az_util import InvalidNameError
from ._az_util import AzureCliError


class KubernetesOperations:
    def __init__(self, config_file=None):
        config.load_kube_config(config_file=config_file)

    @staticmethod
    def get_cluster_name(context):
        """
        Attempts to read the cluster name of an existing Kubernetes cluster through the kube config file.
        :return: String containing the cluster name, or None if not found.
        """
        if not context.env_is_k8s:
            return None
        try:
            with open(os.path.join(os.path.expanduser('~'), '.kube', 'config')) as f:
                config = yaml.load(f)
            return config['contexts'][0]['name']
        except (OSError, KeyError):
            print("Unable to locate kube config file for existing Kubernetes Cluster.")
            return None

    @staticmethod
    def b64encoded_string(text):
        """
        Returns a string representation of a base64 encoded version of the input text.
        Required because the b64encode method only accept/return bytestrings, but json and yaml require strings
        :param text: Text to encode
        :return string: string representation of the encoded text.
        """
        return b64encode(text.encode()).decode()

    def is_deployment_completed(self, name, namespace, desired_replicas):
        """
        Polls Kubernetes to check if a given deployment has finished being deployed.
        :param name: Name of the deployment
        :param namespace: Namespace containing the deployment.
        :param desired_replicas: Number of replicas requested in the deployment.
        :return bool: Returns true if deployment has successfully deployed desired_replicas pods.
        """
        try:
            api_response = client.ExtensionsV1beta1Api().read_namespaced_deployment_status(name=name,
                                                                                           namespace=namespace)
            print("Currently have {0} available replicas".format(api_response.status.available_replicas))
            return api_response.status.available_replicas == desired_replicas
        except ApiException as e:
            print("Exception when calling ExtensionsV1beta1Api->replace_namespaced_deployment_status: %s\n" % e)
            raise e

    def create_deployment(self, deployment_yaml, namespace, deployment_name):
        """
        Starts the creation of a Kubernetes deployment.
        :param deployment_yaml: Path of the yaml file to deploy
        :param namespace: Namespace to create a deployment in.
        :param deployment_name: Name of the new deployment
        :return: None
        """
        k8s_beta = client.ExtensionsV1beta1Api()
        print("Creating deployment {} in namespace {}".format(deployment_name, namespace))
        try:
            resp = k8s_beta.create_namespaced_deployment(namespace=namespace, body=deployment_yaml)
            print("Deployment created. status= {} ".format(str(resp.status)))
        except ApiException as e:
            exc_json = json.loads(e.body)
            if "AlreadyExists" in exc_json['reason']:
                k8s_beta.replace_namespaced_deployment(name=deployment_name, body=deployment_yaml, namespace=namespace)
            else:
                print("An error occurred while creating the deployment. {}".format(exc_json['message']))
                raise

    def deploy_deployment(self, deployment_yaml, max_deployment_time_s, desired_replica_num, secret_name):
        """
        Deploys a Kubernetes Deployment and waits for the deployment to complete.
        :param deployment_yaml: Path of the yaml file to deploy
        :param max_deployment_time_s: Max time to wait for a deployment to succeed before cancelling.
        :param desired_replica_num: Number of replica pods to create in the deployment
        :param secret_name: Name of the Kubernetes secret that contains the ACR login information for the image
                            specified in the deployment_yaml.
        :return bool: True if the deployment succeeds.
        """
        with open(deployment_yaml) as f:
            dep = yaml.load(f)
        namespace = "default"
        deployment_name = dep["metadata"]["name"]
        dep["spec"]["replicas"] = desired_replica_num
        dep["spec"]["template"]["spec"]["imagePullSecrets"][0]["name"] = secret_name
        self.create_deployment(dep, namespace, deployment_name)
        start_time = time.time()
        while time.time() - start_time < max_deployment_time_s:
            print('Deployment Ongoing')
            if self.is_deployment_completed(dep["metadata"]["name"], namespace, desired_replica_num):
                print("Deployment Complete")
                return True
            time.sleep(15)
        print("Deployment failed, to get the desired number of pods")
        return False

    def expose_frontend(self, service_yaml):
        """
        Exposes the azureml-fe deployment as a service.
        :param service_yaml: Path to azureml-fe-service.yaml
        :return: None
        """
        try:
            k8s_core = client.CoreV1Api()
            namespace = 'default'
            with open(service_yaml) as f:
                dep = yaml.load(f)
                print("Exposing frontend on Kubernetes deployment.")
                k8s_core.create_namespaced_service(body=dep, namespace=namespace)

        except ApiException as e:
            exc_json = json.loads(e.body)
            if 'AlreadyExists' in exc_json['reason']:
                return
            print("Exception during service creation: %s" % e)

    def get_service(self, webservicename):
        """
        Retrieves a service with a given webservicename
        :param webservicename: Name of the webservice.
        :return kubernetes.client.V1Service: Returns the webservice specified or None if one was not found.
        """
        try:
            k8s_core = client.CoreV1Api()
            namespace = 'default'
            label_selector = 'webservicename=={}'.format(webservicename)
            api_response = k8s_core.list_namespaced_service(namespace, label_selector=label_selector)
            if len(api_response.items) == 0:
                raise ApiException(status=404, reason="Service with label selector: {} not found".format(label_selector))

            return api_response.items[0]
        except ApiException as e:
            print("Exception occurred while getting a namespaced service. {}".format(e))
            raise

    def delete_service(self, webservicename):
        """
        Deletes a service with a given webservicename
        :param webservicename:
        :return: None
        """
        try:
            k8s_core = client.CoreV1Api()
            namespace = 'default'
            k8s_core.delete_namespaced_service(webservicename, namespace)

        except ApiException as exc:
            print("Exception occurred in delete_service. {}".format(exc))
            raise

    def create_acr_secret_if_not_exist(self, namespace, body):
        """
        Attempts to create an ACR secret on Kubernetes.
        :param namespace: Namespace of the secret.
        :param body: Kubernetes.client.V1Secret containing the acr credentials
        :return bool: True if successful, false if secret already exists.
        """
        retries = 0
        max_retries = 3
        while retries < max_retries:
            try:
                client.CoreV1Api().create_namespaced_secret(namespace, body)
                return True
            except ApiException as e:
                if e.status == 409:  # 409 indicates secret already exists
                    return False
                retries += 1
                if retries >= max_retries:
                    print("Exception occurred in create_acr_secret_if_not_exist: {}".format(e))
                    raise e

    def replace_secrets(self, name, namespace, body):
        """
        Replaces an existing secret. Cannot patch due to immutability.
        :param name: Name of the secret to replace
        :param namespace: Namespace containing the secret
        :param body: Kubernetes.client.V1Secret containing the secret payload
        :return bool: True if successful, false if secret already exists.
        """
        try:
            client.CoreV1Api().delete_namespaced_secret(name, namespace, client.V1DeleteOptions())
            return self.create_acr_secret_if_not_exist(namespace, body)
        except ApiException as e:
            print("Exception occurred in replace_secrets: {}".format(e))
            raise e

    def create_or_replace_docker_secret_if_exists(self, acr_credentials, secret_name):
        """
        Adds a docker registry secret to Kubernetes secret storage.
        :param acr_credentials: Encoded ACR credentials to store as a secret
        :param secret_name: Name of the secret
        :return bool: True if successful.
        """
        print("Creating Secret {}".format(secret_name))
        namespace = 'default'
        secret = dict()
        secret[".dockercfg"] = acr_credentials
        meta = client.V1ObjectMeta(name=secret_name, namespace="default")
        body = client.V1Secret(data=secret, metadata=meta, type="kubernetes.io/dockercfg")
        if self.create_acr_secret_if_not_exist(namespace, body):
            return True
        else:
            return self.replace_secrets(secret_name, namespace, body)

    def encode_acr_credentials(self, acr_host, acr_uname, acr_pwd, acr_email):
        """
        Encodes ACR credentials for correct storage as a .dockerconfigjson secret.
        :param acr_host: Base url of the acr storage
        :param acr_uname: Username of the ACR
        :param acr_pwd: Password of the ACR
        :param acr_email: Email connected to the ACR
        :return string: Base64 representation of ACR credentials
        """
        return self.b64encoded_string(json.dumps(
            {acr_host:
                 {"username": acr_uname,
                  "password": acr_pwd,
                  "email": acr_email,
                  "auth": self.b64encoded_string(acr_uname+":"+acr_pwd)
                  }
             }
        ))

    def add_acr_secret(self, key, server, username, password, email):
        """
        Adds an ACR secret to Kubernetes.
        :param key: Name of the secret being added
        :param server: Base url of the ACR storage
        :param username: Username of the ACR
        :param password: Password of the ACR
        :param email: Email connected to the ACR
        :return: None
        """
        return self.create_or_replace_docker_secret_if_exists(
            self.encode_acr_credentials(server, username, password, email), key)

    def delete_deployment(self, webservicename):
        """
        Deletes a deployment with a given webservicename
        :param webservicename:
        :return: None
        """
        try:
            k8s_core = client.ExtensionsV1beta1Api()
            namespace = 'default'
            delete_options = client.V1DeleteOptions()
            name = webservicename + '-deployment'
            k8s_core.delete_namespaced_deployment(name, namespace, delete_options)
            self.delete_replica_set(name)

        except ApiException as exc:
            print("Exception occurred in delete_deployment. {}".format(exc))
            raise

    def get_filtered_deployments(self, label_selector=''):
        """
        Retrieves a list of deployment objects filtered by the given label_selector
        :param label_selector: Formatted label selector i.e. "webservicename==deployed_service_name"
        :return list[Kubernetes.client.ExtensionsV1beta1Deployment:
        """
        k8s_beta = client.ExtensionsV1beta1Api()
        namespace = 'default'
        try:
            deployment_list = k8s_beta.list_namespaced_deployment(namespace, label_selector=label_selector)
            return deployment_list.items
        except ApiException as exc:
            print("Exception occurred in get_filtered_deployments. ".format(exc))
            raise

    def delete_replica_set(self, deployment_name):
        try:
            print("Deleting replicaset for deployment {}".format(deployment_name))

            # Pipe output of get_rs_proc to grep_named_rs_row_proc
            get_rs_output = subprocess.check_output(['kubectl', 'get', 'rs']).decode('utf-8')
            rs_regex = r'(?P<rs_name>{}-[0-9]+)'.format(deployment_name)
            s = re.search(rs_regex, get_rs_output)
            if s:
                subprocess.check_call(['kubectl', 'delete', 'rs', s.group('rs_name')])
        except subprocess.CalledProcessError as exc:
            print("Unable to delete replica set for deployment {}. {} {}".format(deployment_name, exc, exc.output))
            raise

    def scale_deployment(self, service_name, num_replicas):
        try:
            print("Scaling web service {} to {} pods".format(service_name, num_replicas))
            deployment_name = service_name + '-deployment'
            num_replicas = int(num_replicas)
            subprocess.check_call(['kubectl', 'scale', 'deployment', deployment_name,
                                   '--replicas={}'.format(num_replicas)])
            print("If you increased the number of pods, your service may appear 'Unhealthy' when running")
            print("az ml service list realtime")
            print("This will return to 'Healthy' when all new pods have been created.")
        except subprocess.CalledProcessError:
            print("Unable to scale service. {}")

    def create_service(self, service_yaml, webservicename, webservice_type):
        try:
            k8s_core = client.CoreV1Api()
            namespace = 'default'
            with open(os.path.join(os.path.dirname(__file__), service_yaml)) as f:
                dep = yaml.load(f)
                dep['metadata']['name'] = str(webservicename)
                dep['metadata']['labels']['webservicename'] = str(webservicename)
                dep['metadata']['labels']['azuremlappname'] = str(webservicename)
                dep['metadata']['labels']['webservicetype'] = str(webservice_type)
                dep['spec']['selector']['webservicename'] = str(webservicename)
                print("Payload: {0}".format(dep))
                k8s_core.create_namespaced_service(body=dep, namespace=namespace)
                print("Created service with Name: {0}".format(webservicename))
        except ApiException as e:
            exc_json = json.loads(e.body)
            if 'AlreadyExists' in exc_json['reason']:
                return
            print("Exception during service creation: %s" % e)


def setup_k8s(context, root_name, resource_group, acr_login_server, acr_password, ssh_public_key,
              ssh_private_key_path):
    """

    Creates and configures a new Kubernetes Cluster on Azure with:
    1. Our azureml-fe frontend service.
    2. ACR secrets for our system store and the user's ACR.

    :param root_name: The root name for the environment used to construct the cluster name.
    :param resource_group: The resource group to create the cluster in.
    :param acr_login_server: The base url of the user's ACR.
    :param acr_password: The password for the user's ACR.
    :param ssh_public_key: Value of ssh public key
    :param ssh_private_key_path: str path to private key

    :return: None
    """
    print('Setting up Kubernetes Cluster')
    cluster_name = root_name + "-cluster"
    try:
        if not check_for_kubectl(context):
            return False
        acr_email = az_get_active_email()
        az_create_kubernetes(resource_group, cluster_name, root_name, ssh_public_key)
        az_get_k8s_credentials(resource_group, cluster_name, ssh_private_key_path)
        k8s_ops = KubernetesOperations()
        k8s_ops.add_acr_secret(context.acr_username + 'acrkey', context.acr_username, acr_login_server,
                               acr_password, acr_email)
        deploy_frontend(k8s_ops, acr_email)

    except InvalidNameError as exc:
        print("Invalid cluster name. {}".format(exc.message))
        return False

    except ApiException as exc:
        print("An unexpected exception has occurred. {}".format(exc))
        return False

    except AzureCliError as exc:
        print("An unexpected exception has occurred. {}".format(exc.message))
        return False

    return True


def deploy_frontend(k8s_ops, acr_email):
    k8s_ops.add_acr_secret('amlintfeacrkey', 'azuremlintfe.azurecr.io',
                           'azuremlintfe', 'Zxw+PXQ+KZ1KEEX5172EMc/xN0RTTmyP', acr_email)
    k8s_ops.deploy_deployment(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                           'data', 'azureml-fe-dep.yaml'), 120, 1, 'amlintfeacrkey')
    k8s_ops.expose_frontend(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         'data', 'azureml-fe-service.yaml'))


def check_for_kubectl(context):
    """Checks whether kubectl is present on the system path."""
    try:
        if context.os_is_linux():
            subprocess.check_output('kubectl')
        else:
            subprocess.check_output('kubectl', shell=True)
        return True
    except (subprocess.CalledProcessError, OSError):
        auto_install = input('kubectl is not installed on the path. One click install? (Y/n): ').lower().strip()
        if 'n' not in auto_install and 'no' not in auto_install:
            return az_install_kubectl(context)
        else:
            print('To install Kubectl run the following commands and then re-run az ml env setup')
            print('curl -LO https://storage.googleapis.com/kubernetes-release/release/' +
                  '$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)' +
                  '/bin/linux/amd64/kubectl')
            print('chmod +x ./kubectl')
            print('sudo mv ./kubectl ~/bin')
            return False
