# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
Utilities to interact with the Azure CLI (az).

"""

from builtins import input
import datetime
import json
import requests
import re
import os
import paramiko
import yaml
import errno
import platform
import stat
import adal
from scp import SCPClient
from pkg_resources import resource_string
from azure.cli.core._profile import Profile
from azure.cli.core._config import az_config
from msrestazure.azure_exceptions import CloudError
from azure.cli.core.prompting import prompt_pass
from azure.cli.core.commands import client_factory
from azure.cli.core.commands import LongRunningOperation
import azure.cli.core.azlogging as azlogging
from azure.mgmt.compute.containerservice import ContainerServiceClient
from azure.mgmt.containerregistry.container_registry_management_client import \
    ContainerRegistryManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.resource.resources.models import ResourceGroup
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentProperties
from ._acs_util import _graph_client_factory
from ._acs_util import load_acs_service_principal
from ._acs_util import _validate_service_principal
from ._acs_util import _build_service_principal
from ._acs_util import store_acs_service_principal
from ._acs_util import _create_kubernetes
from ._acs_util import _add_role_assignment


from azure.cli.core.util import get_file_json
from azure.cli.core.util import CLIError

logger = azlogging.get_az_logger(__name__)


# EXCEPTIONS
class Error(Exception):
    """Base class for exceptions raised by this file."""
    pass


class AzureCliError(Error):
    """Exception raised when an Azure CLI operation fails."""

    def __init__(self, message):
        super(AzureCliError, self).__init__()
        self.message = message


class InvalidNameError(Error):
    """Exception raised when the provided environment name does not conform to Azure storage naming rules."""

    def __init__(self, message):
        super(InvalidNameError, self).__init__()
        self.message = message


def validate_env_name(name):
    """
    Validate the given name against Azure storage naming rules
    :param name: The name to validate
    :return: None, if valid. Throws an exception otherwise.
    """
    if not name or len(name) > 20:
        raise InvalidNameError('Name must be between 1 and 20 characters in length.')

    if not bool(re.match('^[a-z0-9]+$', name)):
        raise InvalidNameError(
            'Name must only contain lowercase alphanumeric characters.')


def az_login():
    """Log in to Azure if not already logged in
    :return None
    """
    profile = Profile()

    # interactive login
    try:
        profile.get_subscription()
    except CLIError as exc:
        # thrown when not logged in
        if "'az login'" in str(exc):
            profile.find_subscriptions_on_login(True, None, None, None, None)
        elif "'az account set'" in str(exc):
            # TODO - figure out what to do here..
            raise
        else:
            raise


def az_logout():
    profile = Profile()
    profile.logout_all()


def az_check_subscription():
    """
    Check whether the user wants to use the current default subscription
    Assumes user is logged in to az.
    """

    profile = Profile()
    current_subscription = profile.get_subscription()['name']
    print('Subscription set to {}'.format(current_subscription))
    answer = input('Continue with this subscription (Y/n)? ')
    answer = answer.rstrip().lower()
    if answer == 'n' or answer == 'no':
        print("Available subscriptions:\n  {}".format('\n  '.join(
            [sub['name'] for sub in profile.load_cached_subscriptions()])))
        new_subscription = input('Enter subscription name: ')
        new_subscription = new_subscription.rstrip()
        profile.set_active_subscription(
            profile.get_subscription(new_subscription)['name'])
        print('Active subscription updated to {}'.format(
            profile.get_subscription()['name']))


def az_create_resource_group(context, root_name):
    """Create a resource group using root_name as a prefix"""

    rg_name = root_name + 'rg'
    rg_client = client_factory.get_mgmt_service_client(
        ResourceManagementClient).resource_groups

    if rg_client.check_existence(rg_name):
        print('Resource group {} already exists, skipping creation.'.format(rg_name))
    else:
        print("Creating resource group {}".format(rg_name))
        rg_client.create_or_update(
            rg_name,
            ResourceGroup(location=context.aml_env_default_location)
        )

    return rg_name


def az_register_provider(namespace):
    """ Registers a given resource provider with Azure."""
    client = client_factory.get_mgmt_service_client(ResourceManagementClient).providers
    client.register(namespace)


def get_resource_group_name_by_resource_id(resource_id):
    '''Returns the resource group name from parsing the resource id.
    :param str resource_id: The resource id
    '''
    resource_id = resource_id.lower()
    resource_group_keyword = '/resourcegroups/'
    return resource_id[
           resource_id.index(resource_group_keyword) + len(resource_group_keyword):
           resource_id.index('/providers/')]


def get_acr_api_version():
    return az_config.get('acr', 'apiversion', None)


def az_create_storage_and_acr(root_name, resource_group):
    """
    Create an ACR registry using the Azure CLI (az).
    :param root_name: The prefix to attach to the ACR name.
    :param resource_group: The resource group in which to create the ACR.
    :return: Tuple - the ACR login server, username, and password, storage_name, storage_key
    """
    arm_client = client_factory.get_mgmt_service_client(ResourceManagementClient)
    location = arm_client.resource_groups.get(resource_group).location
    acr_name = root_name + 'acr'
    storage_account_name = root_name + 'stor'

    print(
    'Creating ACR registry and storage account: {} and {} (please be patient, this can take several minutes)'.format(
        acr_name, storage_account_name))
    parameters = {
        'registryName': {'value': acr_name},
        'registryLocation': {'value': location},
        'registrySku': {'value': 'Basic'},
        'adminUserEnabled': {'value': True},
        'storageAccountName': {'value': storage_account_name}
    }
    custom_api_version = get_acr_api_version()
    if custom_api_version:
        parameters['registryApiVersion'] = {'value': custom_api_version}

    template = get_file_json(
        os.path.join(os.path.dirname(__file__), 'data', 'acrtemplate.json'))
    properties = DeploymentProperties(template=template, parameters=parameters,
                                      mode='incremental')
    deployment_client = client_factory.get_mgmt_service_client(
        ResourceManagementClient).deployments
    deployment_name = resource_group + 'deploymentacr' + datetime.datetime.now().strftime(
        '%Y%m%d%I%M%S')

    # deploy via template
    LongRunningOperation()(
        deployment_client.create_or_update(resource_group, deployment_name, properties))

    # fetch finished storage and keys
    storage_client = client_factory.get_mgmt_service_client(
        StorageManagementClient).storage_accounts
    keys = storage_client.list_keys(resource_group, storage_account_name).keys

    # fetch finished registry and credentials
    if custom_api_version:
        acr_client = client_factory.get_mgmt_service_client(
            ContainerRegistryManagementClient,
            api_version=custom_api_version).registries
    else:
        acr_client = client_factory.get_mgmt_service_client(
            ContainerRegistryManagementClient).registries
    registry = acr_client.get(resource_group, acr_name)
    acr_creds = acr_client.list_credentials(resource_group, acr_name)
    return registry.login_server, acr_creds.username, acr_creds.passwords[
        0].value, storage_account_name, keys[0].value


def az_create_acs(root_name, resource_group, acr_login_server, acr_username,
                  acr_password, ssh_public_key):
    """
    Creates an ACS cluster using the Azure CLI and our ARM template. This function should only
    be called after create_acr above. It assumes that the user is already logged in to Azure, and
    that the Azure CLI (az) is present on the system.
    :param root_name: The prefix of the ACS master and agent DNS names.
    :param resource_group: The resource group in which to create the ACS.
    :param acr_login_server: The URL to the ACR that will be connected to this ACS.
    :param acr_username: The username of the ACR connected to this ACS.
    :param acr_password: The password for the above user of ACR.
    :param ssh_public_key: The AML CLI user's public key that will be configured on the masters and agents of this ACS.
    :return: A tuple of DNS names for the ACS master and agent.
    """

    # Load the parameters file
    template = json.loads(
        resource_string(__name__, 'data/acstemplate.json').decode('ascii'))
    parameters = json.loads(
        resource_string(__name__, 'data/acstemplateparameters.json').decode('ascii'))
    deployment_name = resource_group + 'deploymentacs' + datetime.datetime.now().strftime(
        '%Y%m%d%I%M%S')
    acs_master_prefix = root_name + 'acsmaster'
    acs_agent_prefix = root_name + 'acsagent'
    parameters['masterEndpointDNSNamePrefix']['value'] = acs_master_prefix
    parameters['agentpublicEndpointDNSNamePrefix']['value'] = acs_agent_prefix
    parameters['sshRSAPublicKey']['value'] = ssh_public_key
    parameters['azureContainerRegistryName']['value'] = acr_login_server
    parameters['azureContainerRegistryUsername']['value'] = acr_username
    parameters['azureContainerRegistryPassword']['value'] = acr_password

    properties = DeploymentProperties(template=template, parameters=parameters,
                                      mode='incremental')
    client = client_factory.get_mgmt_service_client(ResourceManagementClient).deployments
    client.create_or_update(resource_group, deployment_name, properties, raw=True)

    print(
        'Started ACS deployment. Please note that it can take up to 15 minutes to complete the deployment.')
    print(
        'You can continue to work with az ml in local mode while the ACS is being provisioned.')
    print("To check the status of the deployment, run 'az ml env setup -s {}'".format(
        deployment_name))


def az_get_app_insights_account(completed_deployment):
    """
    Gets the app insights account which has finished deploying. It assumes that the user
    is already logged in to Azure, and that the Azure CLI (az) is present on the system.
    :param completed_deployment: The dictionary object returned from the completed template deployment.
    :return: A tuple of the app insights account name and instrumentation key.
    """
    rp_namespace = 'microsoft.insights'
    resource_type = 'components'
    resource_name = completed_deployment.properties.parameters['appName']['value']
    resource_group = completed_deployment.name.split('deployment')[0]
    rcf = client_factory.get_mgmt_service_client(ResourceManagementClient)
    provider = rcf.providers.get(rp_namespace)
    resource_types = [t for t in provider.resource_types
                      if t.resource_type.lower() == resource_type]
    if len(resource_types) != 1 or not resource_types[0].api_versions:
        raise CLIError('Error finding api version for App Insights.')
    non_preview_versions = [v for v in resource_types[0].api_versions
                            if 'preview' not in v.lower()]
    api_version = non_preview_versions[0] if non_preview_versions else \
        resource_types[0].api_versions[0]
    resource_client = rcf.resources
    result = resource_client.get(resource_group, rp_namespace, '', resource_type,
                                 resource_name, api_version)
    return resource_name, result.properties['InstrumentationKey']


def az_create_app_insights_account(root_name, resource_group):
    """
    Creates an App Insights Account using the Azure CLI and our ARM template.
    It assumes that the user is already logged in to Azure, and
    that the Azure CLI (az) is present on the system.
    :param root_name: The name to use as a prefix for the app insights account
    :param resource_group: The resource group in which to create the app insights account
    :param salt: An optional salt to append to the app insights account name
    """

    az_register_provider('Microsoft.Insights')
    app_insights_account_name = root_name + 'app_ins'

    # Load the parameters file
    template = json.loads(
        resource_string(__name__, 'data/appinsightstemplate.json').decode('ascii'))
    parameters = json.loads(
        resource_string(__name__, 'data/appinsightstemplateparameters.json').decode(
            'ascii'))
    deployment_name = resource_group + 'deploymentappinsights' + datetime.datetime.now().strftime(
        '%Y%m%d%I%M%S')
    parameters['appName']['value'] = app_insights_account_name

    properties = DeploymentProperties(template=template, parameters=parameters,
                                      mode='incremental')
    client = client_factory.get_mgmt_service_client(ResourceManagementClient).deployments
    client.create_or_update(resource_group, deployment_name, properties, raw=True)

    print('Started App Insights Account deployment.')
    return deployment_name


def az_check_template_deployment_status(deployment_name):
    """
    Check the status of a previously started template deployment.
    :param deployment_name: The name of the deployment.
    :return: If deployment succeeded, return the response. If not, display the deployment status.
    """

    # Log in to Azure if not already logged in
    az_login()

    if 'deployment' not in deployment_name:
        raise AzureCliError('Not a valid AML deployment name.')

    resource_group = deployment_name.split('deployment')[0]

    return query_deployment_status(resource_group, deployment_name)


def query_deployment_status(resource_group, deployment_name):
    client = client_factory.get_mgmt_service_client(ResourceManagementClient).deployments
    result = client.get(resource_group, deployment_name)
    if result.properties.provisioning_state == 'Succeeded':
        return result
    elif result.properties.provisioning_state == 'Failed':
        raise AzureCliError('Template deployment failed.')
    print('Deployment status: {}'.format(result.properties.provisioning_state))


def register_acs_providers():
    from azure.mgmt.resource.resources import ResourceManagementClient
    rm_client = client_factory.get_mgmt_service_client(ResourceManagementClient)
    providers = rm_client.providers
    namespaces = ['Microsoft.Network', 'Microsoft.Compute', 'Microsoft.Storage']
    for namespace in namespaces:
        state = providers.get(resource_provider_namespace=namespace)
        if state.registration_state != 'Registered':  # pylint: disable=no-member
            logger.info('registering %s', namespace)
            providers.register(resource_provider_namespace=namespace)
        else:
            logger.info('%s is already registered', namespace)


def az_create_kubernetes(resource_group, cluster_name, dns_prefix, ssh_key_value,
                         service_principal, client_secret):
    """
    Creates a new Kubernetes cluster through az. This can take up to 10 minutes.
    :param resource_group: The name of the resource group to add the cluster to.
    :param cluster_name: The name of the cluster being created
    :param dns_prefix: The dns prefix for the cluster.
    :param ssh_key_value: The absolute path to the ssh key used to set up the cluster.
    :param service_principal: str name of service principal
    :param client_secret: str client secret for service principal

    :return bool: If creation is successful, return true. Otherwise an exception will be raised.
    """
    acs_client = client_factory.get_mgmt_service_client(
        ContainerServiceClient).container_services
    try:
        acs_client.get(resource_group, cluster_name)
        print("Kubernetes cluster with name {} already found. Skipping creation.".format(
            cluster_name))
        return
    except CloudError as exc:
        if 'was not found' not in exc.message:
            raise

    _, subscription_id, _ = Profile().get_login_credentials(subscription_id=None)
    register_acs_providers()
    client = _graph_client_factory()
    if not service_principal:
        principalObj = load_acs_service_principal(subscription_id)
        if principalObj:
            service_principal = principalObj.get('service_principal')
            client_secret = principalObj.get('client_secret')
            _validate_service_principal(client, service_principal)
        else:
            # Nothing to load, make one.
            import binascii
            client_secret = binascii.b2a_hex(os.urandom(10)).decode('utf-8')
            salt = binascii.b2a_hex(os.urandom(3)).decode('utf-8')
            url = 'http://{}.{}.{}.cloudapp.azure.com'.format(salt, dns_prefix, None)

            service_principal = _build_service_principal(client, cluster_name, url,
                                                         client_secret)
            logger.info('Created a service principal: %s', service_principal)
            store_acs_service_principal(subscription_id, client_secret, service_principal)
        if not _add_role_assignment('Contributor', service_principal):
            raise CLIError(
                'Could not create a service principal with the right permissions. Are you an Owner on this project?')
    else:
        # --service-principal specfied, validate --client-secret was too
        if not client_secret:
            raise CLIError('--client-secret is required if --service-principal is specified')
        _validate_service_principal(client, service_principal)

    return _create_kubernetes(resource_group, cluster_name, dns_prefix,
                              cluster_name,
                              ssh_key_value,
                              service_principal=service_principal,
                              client_secret=client_secret)


def _makedirs(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def k8s_install_cli(client_version='latest', install_location=None):
    """
    Downloads the kubectl command line from Kubernetes
    """

    if client_version == 'latest':
        resp = requests.get(
            'https://storage.googleapis.com/kubernetes-release/release/stable.txt')
        resp.raise_for_status()
        client_version = resp.content.decode().strip()

    system = platform.system()
    base_url = 'https://storage.googleapis.com/kubernetes-release/release/{}/bin/{}/amd64/{}'
    if system == 'Windows':
        file_url = base_url.format(client_version, 'windows', 'kubectl.exe')
    elif system == 'Linux':
        # TODO: Support ARM CPU here
        file_url = base_url.format(client_version, 'linux', 'kubectl')
    elif system == 'Darwin':
        file_url = base_url.format(client_version, 'darwin', 'kubectl')
    else:

        raise CLIError('Proxy server ({}) does not exist on the cluster.'.format(system))

    logger.warning('Downloading client to %s from %s', install_location, file_url)
    try:
        with open(install_location, 'wb') as kubectl:
            resp = requests.get(file_url)
            resp.raise_for_status()
            kubectl.write(resp.content)
            os.chmod(install_location,
                     os.stat(
                         install_location).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        logger.warning('Ensure {} is on the path to avoid seeing this message in the future.'.format(install_location))
    except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as err:
        raise CLIError(
            'Connection error while attempting to download client ({})'.format(err))


def az_install_kubectl(context):
    """Downloads kubectl from kubernetes.io and adds it to the system path."""
    executable = 'kubectl' if context.os_is_linux() else 'kubectl.exe'
    full_install_path = os.path.join(os.path.expanduser('~'), 'bin', executable)
    _makedirs(os.path.dirname(full_install_path))
    os.environ['PATH'] += os.pathsep + os.path.dirname(full_install_path)
    k8s_install_cli(install_location=full_install_path)
    return True


def _load_key(key_filename):
    try:
        pkey = paramiko.RSAKey.from_private_key_file(key_filename, None)
    except paramiko.PasswordRequiredException:
        key_pass = prompt_pass('Passphrase for {}:'.format(key_filename))
        pkey = paramiko.RSAKey.from_private_key_file(key_filename, key_pass)
    if pkey is None:
        raise CLIError('failed to load key: {}'.format(key_filename))
    return pkey


def secure_copy(user, host, src, dest,  # pylint: disable=too-many-arguments
                key_filename=os.path.join(os.path.expanduser("~"), '.ssh', 'id_rsa')):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    pkey = _load_key(key_filename)

    ssh.connect(host, username=user, pkey=pkey)
    scp = SCPClient(ssh.get_transport())

    scp.get(src, dest)
    scp.close()


def _handle_merge(existing, addition, key):
    if addition[key]:
        if existing[key] is None:
            existing[key] = addition[key]
            return

        for i in addition[key]:
            if i not in existing[key]:
                existing[key].append(i)


def merge_kubernetes_configurations(existing_file, addition_file):
    try:
        with open(existing_file) as stream:
            existing = yaml.safe_load(stream)
    except (IOError, OSError) as ex:
        if getattr(ex, 'errno', 0) == errno.ENOENT:
            raise CLIError('{} does not exist'.format(existing_file))
        else:
            raise
    except yaml.parser.ParserError as ex:
        raise CLIError('Error parsing {} ({})'.format(existing_file, str(ex)))

    if existing is None:
        raise CLIError(
            'failed to load existing configuration from {}'.format(existing_file))

    try:
        with open(addition_file) as stream:
            addition = yaml.safe_load(stream)
    except (IOError, OSError) as ex:
        if getattr(ex, 'errno', 0) == errno.ENOENT:
            raise CLIError('{} does not exist'.format(existing_file))
        else:
            raise
    except yaml.parser.ParserError as ex:
        raise CLIError('Error parsing {} ({})'.format(addition_file, str(ex)))

    if addition is None:
        raise CLIError(
            'failed to load additional configuration from {}'.format(addition_file))

    _handle_merge(existing, addition, 'clusters')
    _handle_merge(existing, addition, 'users')
    _handle_merge(existing, addition, 'contexts')
    existing['current-context'] = addition['current-context']

    with open(existing_file, 'w+') as stream:
        yaml.dump(existing, stream, default_flow_style=True)


def az_get_k8s_credentials(resource_group, cluster_name, ssh_key_path):
    """
    Downloads Kubernetes config file to the default path of ~/.kube/config
    :param resource_group: Name of resource group that the cluster is in.
    :param cluster_name:  Name of the Kubernetes cluster
    :return: None
    """
    print("Downloading kubeconfig file to {}".format(os.path.expanduser('~')))
    path = os.path.join(os.path.expanduser('~'), '.kube', 'config')
    mgmt_client = client_factory.get_mgmt_service_client(ContainerServiceClient)
    acs_info = mgmt_client.container_services.get(resource_group, cluster_name)
    dns_prefix = acs_info.master_profile.dns_prefix
    location = acs_info.location
    user = acs_info.linux_profile.admin_username
    _makedirs(os.path.dirname(path))

    path_candidate = path
    ix = 0
    while os.path.exists(path_candidate):
        ix += 1
        path_candidate = '{}-{}-{}'.format(path, cluster_name, ix)

    secure_copy(user, '{}.{}.cloudapp.azure.com'.format(dns_prefix, location),
                '.kube/config', path_candidate, key_filename=ssh_key_path)

    # merge things
    if path_candidate != path:
        try:
            merge_kubernetes_configurations(path, path_candidate)
        except yaml.YAMLError as exc:
            logger.warning('Failed to merge credentials to kube config file: %s', exc)
            logger.warning('The credentials have been saved to %s', path_candidate)


def az_get_active_email():
    """
    Retrieves the email address attached to the user who signed in with az login.
    :return: string containing user's email address.
    """
    return Profile().get_subscription()['user']['name']
