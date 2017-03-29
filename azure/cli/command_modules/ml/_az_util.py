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
import re
import subprocess
import uuid
from pkg_resources import resource_filename
from pkg_resources import resource_string
from azure.cli.core._profile import Profile
from azure.cli.core._config import az_config
from azure.cli.core._util import CLIError
from azure.cli.core.commands import client_factory
import azure.cli.core.azlogging as azlogging
from azure.mgmt.containerregistry.container_registry_management_client import ContainerRegistryManagementClient
from azure.mgmt.containerregistry.models import Registry
from azure.mgmt.containerregistry.models.storage_account_properties import StorageAccountProperties
from azure.mgmt.storage.storage_management_client import StorageManagementClient
from azure.mgmt.storage.models import SkuTier
from azure.mgmt.resource.resources.models import ResourceGroup
from azure.mgmt.resource.resources import ResourceManagementClient

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
    from azure.cli.core._util import CLIError
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

    storage_account_name = root_name + 'stor'
    if salt:
        storage_account_name = storage_account_name + salt

    az_register_provider('Microsoft.Storage')
    try:
        print('Creating storage account {}.'.format(storage_account_name))
        storage_create_output = subprocess.check_output(
            ['az', 'storage', 'account', 'create', '-g', resource_group, '-l',
             context.aml_env_default_location, '-n',
             storage_account_name, '--sku', 'Standard_LRS', '-o', 'json'],
            stderr=subprocess.STDOUT).decode('ascii')
    except subprocess.CalledProcessError as exc:
        if 'already taken' in exc.output.decode('ascii'):
            print(
            'A storage account named {} already exists.'.format(storage_account_name))
            salt = str(uuid.uuid4())[:6]
            return az_create_storage_account(context, root_name, resource_group, salt)
        else:
            raise AzureCliError(
                'Error creating storage account. Please report this to deployml@microsoft.com with the following output: {}'  # pylint: disable=line-too-long
                    .format(exc.output))

    try:
        storage_create_output = json.loads(storage_create_output)
        if 'provisioningState' in storage_create_output and storage_create_output[
            'provisioningState'] == 'Succeeded':
            try:
                storage_account_keys = subprocess.check_output(
                    ['az', 'storage', 'account', 'keys', 'list', '-n',
                     storage_account_name, '-g', resource_group,
                     '-o', 'json'],
                    stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as exc:
                raise AzureCliError(
                    'Error retrieving storage account keys: {}'.format(exc.output))

            try:
                storage_account_keys = json.loads(storage_account_keys.decode('ascii'))
            except ValueError:
                raise AzureCliError('Error retrieving storage account keys: {}'.format(
                    storage_account_keys))

            if 'keys' in storage_account_keys:
                storage_account_keys = storage_account_keys['keys']
            else:
                raise AzureCliError(
                    'Error retrieving storage account keys: {}'.format(
                        json.dumps(storage_account_keys)))

            if len(storage_account_keys) != 2:
                raise AzureCliError(
                    'Error retrieving storage account keys: {}'.format(
                        json.dumps(storage_account_keys)))

            if 'keyName' not in storage_account_keys[1] or 'value' not in \
                    storage_account_keys[1]:
                raise AzureCliError(
                    'Error retrieving storage account keys: {}'.format(
                        json.dumps(storage_account_keys)))

            return storage_account_name, storage_account_keys[1]['value']
        else:
            raise AzureCliError(
                'Malformed response while creating storage account. Please report this to deployml@microsoft.com with the following output: {}'  # pylint: disable=line-too-long
                    .format(json.dumps(storage_create_output)))
    except ValueError:
        raise AzureCliError(
            'Malformed response while creating storage account. Please report this to deployml@microsoft.com with the following output: {}'  # pylint: disable=line-too-long
                .format(json.dumps(storage_create_output)))


def az_create_acr(context, root_name, resource_group, storage_account_name):
    """
    Create an ACR registry using the Azure CLI (az).
    :param context: CommandLineInterfaceContext object
    :param root_name: The prefix to attach to the ACR name.
    :param resource_group: The resource group in which to create the ACR.
    :param storage_account_name: The storage account to use for the ACR.
    :return: Tuple - the ACR login server, username, and password
    """

    acr_name = root_name + 'acr'
    logger.info(
    'Creating ACR registry: {} (please be patient, this can take several minutes)'.format(
        acr_name))
    customized_acr_version = az_config.get('acr', 'apiversion', None)
    if customized_acr_version:
        logger.warning('Customized ACR api-version is used: %s', customized_acr_version)
        acr_client = client_factory.get_mgmt_service_client(ContainerRegistryManagementClient,
                                                        api_version=customized_acr_version).registries
    else:
        acr_client = client_factory.get_mgmt_service_client(ContainerRegistryManagementClient).registries

    # get storage account, keys
    storage_client = client_factory.get_mgmt_service_client(StorageManagementClient).storage_accounts
    storage_account = storage_client.get_properties(resource_group, storage_account_name)

    if storage_account.sku.tier == SkuTier.premium: #pylint: disable=no-member
        raise CLIError('Premium storage account {} is currently not supported. ' \
                       'Please use standard storage account.'.format(storage_account_name))

    storage_key = storage_client.list_keys(resource_group, storage_account_name).keys[0].value #pylint: disable=no-member

    # create acr
    registry = acr_client.create_or_update(
        resource_group, acr_name,
        Registry(
            location=context.aml_env_default_location,
            storage_account=StorageAccountProperties(
                storage_account_name,
                storage_key
            ),
            admin_user_enabled=True
        )
    )

    # get acr credential
    acr_creds = acr_client.get_credentials(resource_group, acr_name)

    return registry.login_server, acr_creds.username, acr_creds.password


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
    template_file = resource_filename(__name__, 'data/acstemplate.json')
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

    try:
        subprocess.check_output(
            ['az', 'group', 'deployment', 'create', '-g', resource_group, '-n',
             deployment_name, '--template-file',
             template_file, '--parameters', json.dumps(parameters), '--no-wait'],
            stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        if exc.output:
            result = exc.output.decode('ascii')
            if 'is not valid according' in result:
                print(
                    'ACS provisioning via template failed. This might mean you do not have enough resources in your subscription.')  # pylint: disable=line-too-long
                s = re.search(r"tracking id is '(?P<tracking_id>.+)'", result)
                if s:
                    print('The tracking id is {}.'.format(s.group('tracking_id')))
                    print(
                    'You can login to https://portal.azure.com to find more details about this error.')

        raise AzureCliError('Error creating ACS from template. Error details {}'.format(
            exc.output.decode('ascii')))

    print(
    'Started ACS deployment. Please note that it can take up to 15 minutes to complete the deployment.')
    print(
    'You can continue to work with aml in local mode while the ACS is being provisioned.')
    print("To check the status of the deployment, run 'aml env setup -s {}'".format(
        deployment_name))


def az_parse_acs_outputs(completed_deployment):
    """
    Parses the outputs from a completed acs template deployment.
    :param completed_deployment: The dictionary object returned from the completed template deployment.
    :return: A tuple of DNS names for the ACS master and agent.
    """

    if 'properties' not in completed_deployment or 'outputs' not in completed_deployment[
        'properties']:
        raise AzureCliError(
            'No outputs in deployment. Please report this to deployml@microsoft.com, with the following json in your error report: {}'  # pylint: disable=line-too-long
                .format(json.dumps(completed_deployment)))

    if 'agentpublicFQDN' not in completed_deployment['properties']['outputs'] \
            or 'masterFQDN' not in completed_deployment['properties']['outputs']:
        raise AzureCliError(
            'Malformed output in deployment. Please report this to deployml@microsoft.com, with the following json in your error report: {}'  # pylint: disable=line-too-long
                .format(json.dumps(completed_deployment)))

    if 'value' not in completed_deployment['properties']['outputs']['masterFQDN'] \
            or 'value' not in completed_deployment['properties']['outputs'][
                'agentpublicFQDN']:
        raise AzureCliError(
            'Malformed output in deployment. Please report this to deployml@microsoft.com, with the following json in your error report: {}'  # pylint: disable=line-too-long
                .format(json.dumps(completed_deployment)))

    return completed_deployment['properties']['outputs']['masterFQDN']['value'], \
           completed_deployment['properties']['outputs']['agentpublicFQDN']['value']


def az_get_app_insights_account(completed_deployment):
    """
    Gets the app insights account which has finished deploying. It assumes that the user
    is already logged in to Azure, and that the Azure CLI (az) is present on the system.
    :param completed_deployment: The dictionary object returned from the completed template deployment.
    :return: A tuple of the app insights account name and instrumentation key.
    """

    if 'resourceGroup' not in completed_deployment:
        az_throw_malformed_json_error(completed_deployment)

    if 'properties' not in completed_deployment or 'parameters' not in \
            completed_deployment['properties']:
        az_throw_malformed_json_error(completed_deployment)

    if 'appName' not in completed_deployment['properties'][
        'parameters'] or 'value' not in completed_deployment['properties']['parameters'][
        'appName']:
        az_throw_malformed_json_error(completed_deployment)

    resource_group = completed_deployment['resourceGroup']
    resource_name = completed_deployment['properties']['parameters']['appName']['value']

    try:
        app_insights_get_output = subprocess.check_output(
            ['az', 'resource', 'show', '-g', resource_group, '--resource-type',
             'microsoft.insights/components', '-n', resource_name],
            stderr=subprocess.STDOUT).decode('ascii')
    except subprocess.CalledProcessError as exc:
        raise AzureCliError(
            'Error getting created app insights account. Error details {}'.format(
                exc.output.decode('ascii')))

    try:
        app_insights_get_output = json.loads(app_insights_get_output)

        if 'properties' in app_insights_get_output and 'InstrumentationKey' in \
                app_insights_get_output['properties']:
            return resource_name, app_insights_get_output['properties'][
                'InstrumentationKey']

        az_throw_malformed_json_error(app_insights_get_output)
    except ValueError:
        az_throw_malformed_json_error(app_insights_get_output)


def az_create_app_insights_account(context, root_name, resource_group):
    """
    Creates an App Insights Account using the Azure CLI and our ARM template.
    It assumes that the user is already logged in to Azure, and
    that the Azure CLI (az) is present on the system.
    :param context: CommandLineInterfaceContext object
    :param root_name: The name to use as a prefix for the app insights account
    :param resource_group: The resource group in which to create the app insights account
    :param salt: An optional salt to append to the app insights account name
    """

    az_register_provider('Microsoft.Insights')
    app_insights_account_name = root_name + 'app_ins'

    # Load the parameters file
    template_file = resource_filename(__name__, 'data/appinsightstemplate.json')
    parameters = json.loads(
        resource_string(__name__, 'data/appinsightstemplateparameters.json').decode(
            'ascii'))
    deployment_name = resource_group + 'deploymentappinsights' + datetime.datetime.now().strftime(
        '%Y%m%d%I%M%S')
    parameters['appName']['value'] = app_insights_account_name

    try:
        subprocess.check_output(
            ['az', 'group', 'deployment', 'create', '-g', resource_group, '-n',
             deployment_name, '--template-file',
             template_file, '--parameters', json.dumps(parameters), '--no-wait'],
            stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        if exc.output:
            result = exc.output.decode('ascii')
            if 'is not valid according' in result:
                print(
                    'App Insights Account provisioning via template failed. This might mean you do not have enough resources in your subscription.')  # pylint: disable=line-too-long
                s = re.search(r"tracking id is '(?P<tracking_id>.+)'", result)
                if s:
                    print('The tracking id is {}.'.format(s.group('tracking_id')))
                    print(
                    'You can login to https://portal.azure.com to find more details about this error.')

        raise AzureCliError(
            'Error creating App Insights Account from template. Error details {}'.format(
                exc.output.decode('ascii')))

    print('Started App Insights Account deployment.')
    print("To check the status of the deployment, run 'aml env setup -s {}'".format(
        deployment_name))


def az_check_template_deployment_status(deployment_name):
    """
    Check the status of a previously started template deployment.
    :param deployment_name: The name of the deployment.
    :return: If deployment succeeded, return the dictionary response. If not, display the deployment status.
    """

    # Log in to Azure if not already logged in
    az_login()

    if 'deployment' not in deployment_name:
        raise AzureCliError('Not a valid AML deployment name.')

    resource_group = deployment_name.split('deployment')[0]
    try:
        deployment_status = subprocess.check_output(
            ['az', 'group', 'deployment', 'show', '-g', resource_group, '-n',
             deployment_name, '-o', 'json'],
            stderr=subprocess.STDOUT).decode('ascii')
    except subprocess.CalledProcessError as exc:
        raise AzureCliError(
            'Error retrieving deployment status: {}'.format(exc.output.decode('ascii')))

    try:
        deployment_status = json.loads(deployment_status)
    except ValueError:
        raise AzureCliError(
            'Malformed deployment status. Please report this to deployml@microsoft.com, with the following in your error report: {}'  # pylint: disable=line-too-long
                .format(deployment_status))

    if 'properties' not in deployment_status or 'provisioningState' not in \
            deployment_status['properties']:
        raise AzureCliError(
            'Error retrieving deployment status. Returned object from az cli: {}'.format(
                json.dumps(deployment_status)))

    if deployment_status['properties']['provisioningState'] != 'Succeeded':
        print('Deployment status: {}'.format(
            deployment_status['properties']['provisioningState']))
        return

    return deployment_status


def az_throw_malformed_json_error(json_dump):
    raise AzureCliError(
        'Malformed json response. Please report this to deployml@microsoft.com with the following output: {}'  # pylint: disable=line-too-long
            .format(json.dumps(json_dump)))
