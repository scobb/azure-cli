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
import sys
import time
import dateutil
from dateutil.relativedelta import relativedelta
from scp import SCPClient
import uuid
from pkg_resources import resource_string
from azure.cli.core._profile import Profile
from azure.cli.core._profile import CLOUD
from azure.cli.core._config import az_config
from azure.cli.core._environment import get_config_dir
from azure.cli.core.prompting import prompt_pass
from azure.cli.core.commands import client_factory
from azure.cli.core.commands import LongRunningOperation
import azure.cli.core.azlogging as azlogging
from azure.mgmt.compute.compute_management_client import ComputeManagementClient
from azure.mgmt.containerregistry.container_registry_management_client import ContainerRegistryManagementClient
from azure.mgmt.storage.storage_management_client import StorageManagementClient
from azure.mgmt.resource.resources.models import ResourceGroup
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentProperties
try:
    from azure.cli.core.util import get_file_json
    from azure.cli.core.util import CLIError
    from azure.cli.core.util import shell_safe_json_parse
except ImportError:
    from azure.cli.core._util import get_file_json
    from azure.cli.core._util import CLIError
    from azure.cli.core._util import shell_safe_json_parse

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
    """Log in to Azure if not already logged in"""
    profile = Profile()
    try:
        profile.get_subscription()
    except CLIError as exc:
        # thrown when not logged in
        if "'az login'" in exc.message:
            profile.find_subscriptions_on_login(True, None, None, None, None)
        elif "'az account set'" in exc.message:
            # TODO - figure out what to do here..
            raise
        else:
            raise


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
    rg_client = client_factory.get_mgmt_service_client(ResourceManagementClient).resource_groups

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


def az_create_storage_account(context, root_name, resource_group, salt=None):
    """
    Create a storage account for the AML environment.
    :param context: CommandLineInterfaceContext object
    :param root_name: The name to use as a prefix for the storage account.
    :param resource_group: The resource group in which to create the storage account.
    :param salt: An optional salt to append to the storage account name.
    :return: string - the name of the storage account created, if successful.
    """

    from azure.mgmt.storage.models import \
        (StorageAccountCreateParameters, Sku)
    storage_account_name = root_name + 'stor'
    if salt:
        storage_account_name = storage_account_name + salt

    az_register_provider('Microsoft.Storage')

    print('Creating storage account {}.'.format(storage_account_name))
    client = client_factory.get_mgmt_service_client(StorageManagementClient).storage_accounts
    client.create(resource_group, storage_account_name,
                  StorageAccountCreateParameters(
                      location=context.aml_env_default_location,
                      sku=Sku('Standard_LRS'),
                      kind='Storage',
                  )).wait()
    keys = client.list_keys(resource_group, storage_account_name).keys

    return storage_account_name, keys[0].value


def get_resource_group_name_by_resource_id(resource_id):
    '''Returns the resource group name from parsing the resource id.
    :param str resource_id: The resource id
    '''
    resource_id = resource_id.lower()
    resource_group_keyword = '/resourcegroups/'
    return resource_id[resource_id.index(resource_group_keyword) + len(resource_group_keyword):
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

    print('Creating ACR registry and storage account: {} and {} (please be patient, this can take several minutes)'.format(
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

    template = get_file_json(os.path.join(os.path.dirname(__file__), 'data', 'acrtemplate.json'))
    properties = DeploymentProperties(template=template, parameters=parameters, mode='incremental')
    deployment_client = client_factory.get_mgmt_service_client(ResourceManagementClient).deployments
    deployment_name = resource_group + 'deploymentacr' + datetime.datetime.now().strftime(
        '%Y%m%d%I%M%S')

    # deploy via template
    LongRunningOperation()(deployment_client.create_or_update(resource_group, deployment_name, properties))

    # fetch finished storage and keys
    storage_client = client_factory.get_mgmt_service_client(StorageManagementClient).storage_accounts
    keys = storage_client.list_keys(resource_group, storage_account_name).keys

    # fetch finished registry and credentials
    if custom_api_version:
        acr_client = client_factory.get_mgmt_service_client(ContainerRegistryManagementClient,
                                                            api_version=custom_api_version).registries
    else:
        acr_client = client_factory.get_mgmt_service_client(ContainerRegistryManagementClient).registries
    registry = acr_client.get(resource_group, acr_name)
    acr_creds = acr_client.list_credentials(resource_group, acr_name)
    return registry.login_server, acr_creds.username, acr_creds.passwords[0].value, storage_account_name, keys[0].value


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
    template = json.loads(resource_string(__name__, 'data/acstemplate.json').decode('ascii'))
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

    properties = DeploymentProperties(template=template, parameters=parameters, mode='incremental')
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

    properties = DeploymentProperties(template=template, parameters=parameters, mode='incremental')
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


def az_create_kubernetes(resource_group, cluster_name, dns_prefix, ssh_key_path):
    """
    Creates a new Kubernetes cluster through az. This can take up to 10 minutes.
    :param resource_group: The name of the resource group to add the cluster to.
    :param cluster_name: The name of the cluster being created
    :param dns_prefix: The dns prefix for the cluster.
    :param ssh_key_path: The absolute path to the ssh key used to set up the cluster.

    :return bool: If creation is successful, return true. Otherwise an exception will be raised.
    """
    from azure.mgmt.compute.containerservice import ContainerServiceClient
    from msrestazure.azure_exceptions import CloudError
    with open(ssh_key_path, 'r') as ssh_key_file:
        ssh_key_value = ssh_key_file.read()

    client = client_factory.get_mgmt_service_client(ContainerServiceClient).container_services
    try:
        client.get(resource_group, cluster_name)
        print("Kubernetes cluster with name {} already found. Skipping creation.".format(cluster_name))
        return
    except CloudError as exc:
        if 'was not found' not in exc.message:
            raise

    _, subscription_id, _ = Profile().get_login_credentials(subscription_id=None)

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
    auth_obj, sub_id, tenant_id = Profile().get_login_credentials()

    # print('auth_obj: {}'.format(auth_obj))
    # print('auth_obj.header: {}'.format(auth_obj.header))
    # print('tenant_id: {}'.format(tenant_id))
    session = auth_obj.signed_session()
    # print('session: {}'.format(session))
    # print('session.headers: {}'.format(session.headers))
    client_secret = session.headers["Authorization"].replace("Bearer ", "")
    print Profile().get_subscription()
    _create_kubernetes(resource_group, cluster_name, dns_prefix, cluster_name, ssh_key_value,
                       client_secret=client_secret, service_principal=tenant_id)
    # from azure.graphrbac import GraphRbacManagementClient
    # cred, _, tenant_id = Profile().get_login_credentials(resource=CLOUD.endpoints.active_directory_graph_resource_id)
    # client = GraphRbacManagementClient(cred, tenant_id,
    #                                    base_url=CLOUD.endpoints.active_directory_graph_resource_id)
    #
    # from azure.cli.core.commands.client_factory import configure_common_settings
    # configure_common_settings(client)
    # principalObj = load_acs_service_principal(subscription_id)
    # print('principalObj: {}'.format(principalObj))
    #
    # if principalObj:
    #     service_principal = principalObj.get('service_principal')
    #     client_secret = principalObj.get('client_secret')
    #     _validate_service_principal(client, service_principal)
    # else:
    #     # Nothing to load, make one.
    #     import binascii
    #     client_secret = binascii.b2a_hex(os.urandom(10)).decode('utf-8')
    #     salt = binascii.b2a_hex(os.urandom(3)).decode('utf-8')
    #     url = 'http://{}.{}.{}.cloudapp.azure.com'.format(salt, dns_prefix, None)
    #
    #     service_principal = _build_service_principal(client, name, url, client_secret)
    #     logger.info('Created a service principal: %s', service_principal)
    #     store_acs_service_principal(subscription_id, client_secret, service_principal)
    # # Either way, update the role assignment, this fixes things if we fail part-way through
    # if not _add_role_assignment('Contributor', service_principal):
    #     raise CLIError(
    #         'Could not create a service principal with the right permissions. Are you an Owner on this project?')
    # # Create new K8s cluster
    # print("Creating kubernetes cluster. This can take up to 10 minutes.")
    # k8s_create = subprocess.Popen(
    #     ['az', 'acs', 'create',
    #      '--orchestrator-type=kubernetes',
    #      '--resource-group=' + resource_group,
    #      '--name=' + cluster_name,
    #      '--dns-prefix=' + dns_prefix,
    #      '--ssh-key-value=' + ssh_key_path + '.pub'],
    #     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # output, err = k8s_create.communicate()
    # output = output.decode('ascii')
    # if err or '"provisioningState": "Succeeded"' not in output:
    #     result = err.decode('ascii')
    #     print('Provisioning of kubernetes cluster failed. {}'.format(result))
    #     raise AzureCliError('Provisioning of kubernetes cluster failed. {}'.format(result))

# ============ BEGIN ACS ================#


def _create_kubernetes(resource_group_name, deployment_name, dns_name_prefix,
                       name, ssh_key_value,
                       admin_username="azureuser", agent_count="3",
                       agent_vm_size="Standard_D2_v2",
                       location=None, service_principal=None, client_secret=None,
                       master_count="1",
                       windows=False, admin_password='', validate=True,
                       no_wait=False):
    print("Creating k8s...")
    if not location:
        location = '[resourceGroup().location]'
    windows_profile = None
    os_type = 'Linux'
    if windows:
        if len(admin_password) == 0:
            raise CLIError('--admin-password is required.')
        if len(admin_password) < 6:
            raise CLIError('--admin-password must be at least 6 characters')
        windows_profile = {
            "adminUsername": admin_username,
            "adminPassword": admin_password,
        }
        os_type = 'Windows'

    template = {
        "$schema": "https://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {
            "clientSecret": {
                "type": "secureString",
                "metadata": {
                    "description": "The client secret for the service principal"
                }
            }
        },
        "resources": [
            {
                "apiVersion": "2017-01-31",
                "location": location,
                "type": "Microsoft.ContainerService/containerServices",
                "name": name,
                "properties": {
                    "orchestratorProfile": {
                        "orchestratorType": "kubernetes"
                    },
                    "masterProfile": {
                        "count": master_count,
                        "dnsPrefix": dns_name_prefix
                    },
                    "agentPoolProfiles": [
                        {
                            "name": "agentpools",
                            "count": agent_count,
                            "vmSize": agent_vm_size,
                            "dnsPrefix": dns_name_prefix + '-k8s-agents',
                            "osType": os_type,
                        }
                    ],
                    "linuxProfile": {
                        "ssh": {
                            "publicKeys": [
                                {
                                    "keyData": ssh_key_value
                                }
                            ]
                        },
                        "adminUsername": admin_username
                    },
                    "windowsProfile": windows_profile,
                    "servicePrincipalProfile": {
                        "ClientId": service_principal,
                        "Secret": "[parameters('clientSecret')]"
                    }
                }
            }
        ]
    }
    params = {
        "clientSecret": {
            "value": client_secret
        }
    }

    deployment = _invoke_deployment(resource_group_name, deployment_name, template, params, validate, no_wait)
    print(deployment)
    print(deployment.error)
    print(deployment.properties)


def _invoke_deployment(resource_group_name, deployment_name, template, parameters, validate, no_wait):
    from azure.mgmt.resource.resources import ResourceManagementClient
    from azure.mgmt.resource.resources.models import DeploymentProperties
    print('Invoking deployment...')
    print('no_wait: {}'.format(no_wait))
    properties = DeploymentProperties(template=template, parameters=parameters, mode='incremental')
    smc = client_factory.get_mgmt_service_client(ResourceManagementClient).deployments
    if validate:
        logger.info('==== BEGIN TEMPLATE ====')
        logger.info(json.dumps(template, indent=2))
        logger.info('==== END TEMPLATE ====')
        return smc.validate(resource_group_name, deployment_name, properties)
    return smc.create_or_update(resource_group_name, deployment_name, properties, raw=no_wait)

def _auth_client_factory(scope=None):
    import re
    from azure.cli.core.commands.client_factory import get_mgmt_service_client
    from azure.mgmt.authorization import AuthorizationManagementClient
    subscription_id = None
    if scope:
        matched = re.match('/subscriptions/(?P<subscription>[^/]*)/', scope)
        if matched:
            subscription_id = matched.groupdict()['subscription']
    return get_mgmt_service_client(AuthorizationManagementClient, subscription_id=subscription_id)


def _graph_client_factory(**_):
    from azure.cli.core._profile import Profile, CLOUD
    from azure.cli.core.commands.client_factory import configure_common_settings
    from azure.graphrbac import GraphRbacManagementClient
    profile = Profile()
    cred, _, tenant_id = profile.get_login_credentials(
        resource=CLOUD.endpoints.active_directory_graph_resource_id)
    client = GraphRbacManagementClient(cred,
                                       tenant_id,
                                       base_url=CLOUD.endpoints.active_directory_graph_resource_id)
    configure_common_settings(client)
    return client


def _acs_client_factory(_):
    from azure.cli.core.profiles import ResourceType
    from azure.cli.core.commands.client_factory import get_mgmt_service_client
    return get_mgmt_service_client(ResourceType.MGMT_CONTAINER_SERVICE).container_services


def _resolve_role_id(role, scope, definitions_client):
    role_id = None
    try:
        uuid.UUID(role)
        role_id = role
    except ValueError:
        pass
    if not role_id:  # retrieve role id
        role_defs = list(definitions_client.list(scope, "roleName eq '{}'".format(role)))
        if not role_defs:
            raise CLIError("Role '{}' doesn't exist.".format(role))
        elif len(role_defs) > 1:
            ids = [r.id for r in role_defs]
            err = "More than one role matches the given name '{}'. Please pick a value from '{}'"
            raise CLIError(err.format(role, ids))
        role_id = role_defs[0].id
    return role_id


def _get_object_stubs(graph_client, assignees):
    from azure.graphrbac.models.get_objects_parameters import GetObjectsParameters
    params = GetObjectsParameters(include_directory_object_references=True,
                                  object_ids=assignees)
    return list(graph_client.objects.get_objects_by_object_ids(params))


def _resolve_object_id(assignee):
    client = _graph_client_factory()
    result = None
    if assignee.find('@') >= 0:  # looks like a user principal name
        result = list(client.users.list(filter="userPrincipalName eq '{}'".format(assignee)))
    if not result:
        result = list(client.service_principals.list(
            filter="servicePrincipalNames/any(c:c eq '{}')".format(assignee)))
    if not result:  # assume an object id, let us verify it
        result = _get_object_stubs(client, [assignee])

    # 2+ matches should never happen, so we only check 'no match' here
    if not result:
        raise CLIError("No matches in graph database for '{}'".format(assignee))

    return result[0].object_id


def _build_role_scope(resource_group_name, scope, subscription_id):
    subscription_scope = '/subscriptions/' + subscription_id
    if scope:
        if resource_group_name:
            err = 'Resource group "{}" is redundant because scope is supplied'
            raise CLIError(err.format(resource_group_name))
    elif resource_group_name:
        scope = subscription_scope + '/resourceGroups/' + resource_group_name
    else:
        scope = subscription_scope
    return scope



def create_role_assignment(role, assignee, resource_group_name=None, scope=None):
    return _create_role_assignment(role, assignee, resource_group_name, scope)


def _create_role_assignment(role, assignee, resource_group_name=None, scope=None,  # pylint: disable=too-many-arguments
                            resolve_assignee=True):
    factory = _auth_client_factory(scope)
    assignments_client = factory.role_assignments
    definitions_client = factory.role_definitions

    scope = _build_role_scope(resource_group_name, scope,
                              assignments_client.config.subscription_id)

    role_id = _resolve_role_id(role, scope, definitions_client)
    object_id = _resolve_object_id(assignee) if resolve_assignee else assignee
    properties = RoleAssignmentProperties(role_id, object_id)
    assignment_name = uuid.uuid4()
    custom_headers = None
    return assignments_client.create(scope, assignment_name, properties,
                                     custom_headers=custom_headers)

def _add_role_assignment(role, service_principal, delay=2, output=True):
    # AAD can have delays in propagating data, so sleep and retry
    if output:
        sys.stdout.write('waiting for AAD role to propagate.')
    for x in range(0, 10):
        try:
            # TODO: break this out into a shared utility library
            create_role_assignment(role, service_principal)
            break
        except CloudError as ex:
            if ex.message == 'The role assignment already exists.':
                break
            logger.info('%s', ex.message)
        except:  # pylint: disable=bare-except
            pass
        if output:
            sys.stdout.write('.')
            time.sleep(delay + delay * x)
    else:
        return False
    if output:
        print('done')
    return True

def store_acs_service_principal(subscription_id, client_secret, service_principal,
                                config_path=os.path.join(get_config_dir(),
                                                         'acsServicePrincipal.json')):
    obj = {}
    if client_secret:
        obj['client_secret'] = client_secret
    if service_principal:
        obj['service_principal'] = service_principal

    fullConfig = load_acs_service_principals(config_path=config_path)
    if not fullConfig:
        fullConfig = {}
    fullConfig[subscription_id] = obj

    with os.fdopen(os.open(config_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600),
                   'w+') as spFile:
        json.dump(fullConfig, spFile)


def load_acs_service_principal(subscription_id, config_path=os.path.join(get_config_dir(),
                                                                         'acsServicePrincipal.json')):
    config = load_acs_service_principals(config_path)
    if not config:
        return None
    return config.get(subscription_id)


def _build_service_principal(client, name, url, client_secret):
    sys.stdout.write('creating service principal')
    result = create_application(client.applications, name, url, [url], password=client_secret)
    service_principal = result.app_id  # pylint: disable=no-member
    for x in range(0, 10):
        try:
            create_service_principal(service_principal)
        # TODO figure out what exception AAD throws here sometimes.
        except:  # pylint: disable=bare-except
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(2 + 2 * x)
    print('done')
    return service_principal


def _build_application_creds(password=None, key_value=None, key_type=None,  # pylint: disable=too-many-arguments
                             key_usage=None, start_date=None, end_date=None):
    if password and key_value:
        raise CLIError('specify either --password or --key-value, but not both.')
    if not start_date:
        start_date = datetime.datetime.utcnow()
    elif isinstance(start_date, str):
        start_date = dateutil.parser.parse(start_date)

    if not end_date:
        end_date = start_date + relativedelta(years=1)
    elif isinstance(end_date, str):
        end_date = dateutil.parser.parse(end_date)  # pylint: disable=redefined-variable-type

    key_type = key_type or 'AsymmetricX509Cert'
    key_usage = key_usage or 'Verify'

    password_creds = None
    key_creds = None
    if password:
        password_creds = [PasswordCredential(start_date, end_date, str(uuid.uuid4()), password)]
    elif key_value:
        key_creds = [KeyCredential(start_date, end_date, key_value, str(uuid.uuid4()),
                                   key_usage, key_type)]

    return (password_creds, key_creds)


def create_application(client, display_name, homepage, identifier_uris,  # pylint: disable=too-many-arguments
                       available_to_other_tenants=False, password=None, reply_urls=None,
                       key_value=None, key_type=None, key_usage=None, start_date=None,
                       end_date=None):
    password_creds, key_creds = _build_application_creds(password, key_value, key_type,
                                                         key_usage, start_date, end_date)

    app_create_param = ApplicationCreateParameters(available_to_other_tenants,
                                                   display_name,
                                                   identifier_uris,
                                                   homepage=homepage,
                                                   reply_urls=reply_urls,
                                                   key_credentials=key_creds,
                                                   password_credentials=password_creds)
    return client.create(app_create_param)
def _resolve_service_principal(client, identifier):
    # todo: confirm with graph team that a service principal name must be unique
    result = list(client.list(filter="servicePrincipalNames/any(c:c eq '{}')".format(identifier)))
    if result:
        return result[0].object_id
    try:
        uuid.UUID(identifier)
        return identifier  # assume an object id
    except ValueError:
        raise CLIError("service principal '{}' doesn't exist".format(identifier))
def show_service_principal(client, identifier):
    object_id = _resolve_service_principal(client, identifier)
    return client.get(object_id)
def _validate_service_principal(client, sp_id):
    # discard the result, we're trusting this to throw if it can't find something
    try:
        show_service_principal(client.service_principals, sp_id)
    except:  # pylint: disable=bare-except
        raise CLIError(
            'Failed to validate service principal, if this persists try deleting $HOME/.azure/acsServicePrincipal.json')

def load_acs_service_principals(config_path):
    if not os.path.exists(config_path):
        return None
    fd = os.open(config_path, os.O_RDONLY)
    try:
        with os.fdopen(fd) as f:
            return shell_safe_json_parse(f.read())
    except:  # pylint: disable=bare-except
        return None

# ==================END ACS==================== #
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
        resp = requests.get('https://storage.googleapis.com/kubernetes-release/release/stable.txt')
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
                     os.stat(install_location).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as err:
        raise CLIError('Connection error while attempting to download client ({})'.format(err))


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
        raise CLIError('failed to load existing configuration from {}'.format(existing_file))

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
        raise CLIError('failed to load additional configuration from {}'.format(addition_file))

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
    mgmt_client = client_factory.get_mgmt_service_client(ComputeManagementClient)
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
