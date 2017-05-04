import requests
import subprocess
import socket
import os
import platform
import time
import types
from collections import OrderedDict
from builtins import input
from builtins import next
from azure.cli.core.util import CLIError
from ._util import CommandLineInterfaceContext
from ._util import acs_connection_timeout
from ._util import create_ssh_key_if_not_exists
from ._util import InvalidConfError
from .service._realtimeutilities import check_marathon_port_forwarding
from ._az_util import az_check_template_deployment_status
from ._az_util import AzureCliError
from ._az_util import az_get_app_insights_account
from ._az_util import validate_env_name
from ._az_util import InvalidNameError
from ._az_util import az_login
from ._az_util import az_check_subscription
from ._az_util import az_create_resource_group
from ._az_util import az_create_storage_and_acr
from ._az_util import az_create_app_insights_account
from ._az_util import az_create_acs
from ._az_util import query_deployment_status
from ._az_util import az_get_k8s_credentials
from ._k8s_util import KubernetesOperations
from ._k8s_util import setup_k8s
from ..ml import __version__


def version():
    print('Azure Machine Learning Command Line Tools {}'.format(__version__))


def env_about():
    """Help on setting up an AML environment."""

    print("""
    Azure Machine Learning Command Line Tools

    Environment Setup
    This CLI helps you create and manage Azure Machine Learning web services. The CLI
    can be used in either local or cluster modes.


    Local mode:
    To enter local mode: az ml env local

    In local mode, the CLI can be used to create locally running web services for development
    and testing. In order to run the CLI in local mode, you will need the following environment
    variables defined:

    AML_STORAGE_ACCT_NAME : Set this to an Azure storage account.
                            See https://docs.microsoft.com/en-us/azure/storage/storage-introduction for details.
    AML_STORAGE_ACCT_KEY  : Set this to either the primary or secondary key of the above storage account.
    AML_ACR_HOME          : Set this to the URL of your Azure Container Registry (ACR).
                            See https://docs.microsoft.com/en-us/azure/container-registry/container-registry-intro
                            for details.
    AML_ACR_USER          : Set this to the username of the above ACR.
    AML_ACR_PW            : Set this to the password of the above ACR.
    AML_APP_INSIGHTS_NAME : Set this to an App Insights account.
    AML_APP_INSIGHTS_KEY  : Set this to an App Insights instrumentation key.


    Cluster mode:
    To enter cluster mode: az ml env cluster

    In cluster mode, the CLI can be used to deploy production web services. Realtime web services are deployed to
    an Azure Container Service (ACS) cluster, and batch web services are deployed to an HDInsight Spark cluster. In
    order to use the CLI in cluster mode, define the following environment variables (in addition to those above for
    local mode):

    AML_ACS_MASTER        : Set this to the URL of your ACS Master (e.g.yourclustermgmt.westus.cloudapp.azure.com).
    AML_ACS_AGENT         : Set this to the URL of your ACS Public Agent (e.g. yourclusteragents.westus.cloudapp.azure.com).
    AML_HDI_CLUSTER       : Set this to the URL of your HDInsight Spark cluster.
    AML_HDI_USER          : Set this to the admin user of your HDInsight Spark cluster.
    AML_HDI_PW            : Set this to the password of the admin user of your HDInsight Spark cluster.
    AML_ACS_IS_K8S        ; Set this to 'true' if the ACS cluster is running Kubernetes
    """)


def env_cluster(verb, context=CommandLineInterfaceContext()):
    """Switches environment to cluster mode."""
    try:
        conf = context.read_config()
        if not conf:
            conf = {}
    except InvalidConfError:
        if verb:
            print('[Debug] Suspicious content in ~/.amlconf.')
            print('[Debug] Resetting.')
        conf = {}

    if not context.env_is_k8s:
        try:
            forwarded_port = context.check_marathon_port_forwarding()
            acs_is_set_up = forwarded_port > 0
        except:
            acs_is_set_up = False
        if not acs_is_set_up:
            continue_without_acs = context.get_input(
                'Could not connect to ACS cluster. Continue with cluster mode anyway (y/N)? ')
            continue_without_acs = continue_without_acs.strip().lower()
            if continue_without_acs != 'y' and continue_without_acs != 'yes':
                print(
                    "Aborting switch to cluster mode. Please run 'az ml env about' for more information on setting up your cluster.")  # pylint: disable=line-too-long
                return
    else:
        basename = context.az_account_name[:-4]
        ssh_key_path = os.path.join(os.path.expanduser('~'), '.ssh', 'acs_id_rsa')
        if not os.path.exists(ssh_key_path):
            print('Unable to find ssh key {}. If you did not provision this Kubernetes '
                  'environment from this machine, you may need to copy the key from '
                  'the provisioning machine.'.format(ssh_key_path))
            return
        az_get_k8s_credentials('{}rg'.format(basename), '{}-cluster'.format(basename), ssh_key_path)

    conf['mode'] = 'cluster'
    context.write_config(conf)

    print("Running in cluster mode.")
    env_describe(context)


def env_describe(context=CommandLineInterfaceContext()):
    """Print current environment settings."""
    if context.in_local_mode():
        print("")
        print("** Warning: Running in local mode. **")
        print("To switch to cluster mode: az ml env cluster")
        print("")

    print('Storage account name   : {}'.format(context.az_account_name))
    print('Storage account key    : {}'.format(context.az_account_key))
    print('ACR URL                : {}'.format(context.acr_home))
    print('ACR username           : {}'.format(context.acr_user))
    print('ACR password           : {}'.format(context.acr_pw))
    print('App Insights account   : {}'.format(context.app_insights_account_name))
    print('App Insights key       : {}'.format(context.app_insights_account_key))

    if not context.in_local_mode():
        print('HDI cluster URL        : {}'.format(context.hdi_home))
        print('HDI admin user name    : {}'.format(context.hdi_user))
        print('HDI admin password     : {}'.format(context.hdi_pw))
        if context.env_is_k8s:
            print('Using Kubernetes       : {}'.format(os.environ.get('AML_ACS_IS_K8S')))
        else:
            print('ACS Master URL         : {}'.format(context.acs_master_url))
            print('ACS Agent URL          : {}'.format(context.acs_agent_url))


def env_local(verb, context=CommandLineInterfaceContext()):

    if not context.os_is_linux():
        print('Local mode is not supported on this platform.')
        return

    try:
        conf = context.read_config()
        if not conf:
            if verb:
                print('[Debug] No configuration file found.')
            conf = {}
        elif 'mode' not in conf and verb:
            print('[Debug] No mode setting found in config file. Suspicious.')
        conf['mode'] = 'local'
    except InvalidConfError:
        if verb:
            print('[Debug] Suspicious content in ~/.amlconf.')
            print('[Debug] Resetting.')
        conf = {'mode': 'local'}

    context.write_config(conf)
    env_describe(context)
    return


def write_acs_to_amlenvrc(acs_master, acs_agent, env_verb):
    env_statements = ["{} AML_ACS_MASTER={}".format(env_verb, acs_master),
                      "{} AML_ACS_AGENT={}".format(env_verb, acs_agent)]

    print('\n'.join([' {}'.format(statement) for statement in env_statements]))
    try:
        with open(os.path.expanduser('~/.amlenvrc'), 'a+') as env_file:
            env_file.write('\n'.join(env_statements) + '\n')
    except IOError:
        pass

    print('')


def env_setup(status, name, kubernetes, local_only, service_principal_app_id,
              service_principal_password, context=CommandLineInterfaceContext()):
    if status:
        try:
            completed_deployment = az_check_template_deployment_status(status)
        except AzureCliError as exc:
            print(exc.message)
            return

        if completed_deployment:
            try:
                acs_master = completed_deployment.properties.outputs['masterFQDN']['value']
                acs_agent = completed_deployment.properties.outputs['agentpublicFQDN']['value']
                if acs_master and acs_agent:
                    print('ACS deployment succeeded.')
                    print('ACS Master URL     : {}'.format(acs_master))
                    print('ACS Agent URL      : {}'.format(acs_agent))
                    print('ACS admin username : acsadmin (Needed to set up port forwarding in cluster mode).')
                    print('To configure az ml with this environment, set the following environment variables.')
                    if platform.system() in ['Linux', 'linux', 'Unix', 'unix']:
                        write_acs_to_amlenvrc(acs_master, acs_agent, "export")
                    else:
                        write_acs_to_amlenvrc(acs_master, acs_agent, "set")

                    try:
                        ssh_config_fp = os.path.join(os.path.expanduser('~'), '.ssh', 'config')
                        with open(ssh_config_fp, 'a+') as sshconf:
                            sshconf.write('Host {}\n'.format(acs_master))
                            sshconf.write('    HostName {}\n'.format(acs_master))
                            sshconf.write('    User acsadmin\n')
                            sshconf.write('    IdentityFile ~/.ssh/acs_id_rsa\n')
                        os.chmod(ssh_config_fp, 0o600)
                    except:
                        print('Failed to update ~/.ssh/config. '
                              'You will need to manually update your '
                              '.ssh/config to look for ~/.ssh/acs_id_rsa '
                              'for host {}'.format(acs_master))

                    print("To switch to cluster mode, run 'az ml env cluster'.")
            except AzureCliError as exc:
                print(exc.message)

        return

    try:
        ssh_private_key_path, ssh_public_key = create_ssh_key_if_not_exists()
    except AzureCliError:
        return

    if local_only:
        print(
        'Setting up your Azure ML environment with a storage account, App Insights account, and ACR registry.')
    else:
        print('Setting up your Azure ML environment with a storage account, App Insights account, ACR registry and ACS cluster.')

    if not name:
        root_name = input('Enter environment name (1-20 characters, lowercase alphanumeric): ')
        try:
            validate_env_name(root_name)
        except InvalidNameError as e:
            print('Invalid environment name. {}'.format(e.message))
            return
    else:
        root_name = name
    if service_principal_app_id and not service_principal_password:
        raise CLIError('When deploying with service principal, password (-p) must be specified.')
    az_login()
    az_check_subscription()
    resource_group = az_create_resource_group(context, root_name)

    app_insight_values_to_check = OrderedDict([
            ('App Insights Account Name', context.app_insights_account_name),
            ('App Insights Account Key', context.app_insights_account_key)
        ])
    app_insight_args = [root_name, resource_group]
    app_insights_deployment_id = create_action_with_prompt_if_defined(
        context,
        'App Insights Account',
        app_insight_values_to_check,
        az_create_app_insights_account,
        app_insight_args
    )

    acr_values_to_check = OrderedDict([
        ('ACR Login Server', context.acr_home),
        ('ACR Username', context.acr_user),
        ('ACR Password', context.acr_pw),
        ('Storage Account', context.az_account_name),
        ('Storage Key', context.az_account_key)]
    )
    acr_args = [root_name, resource_group]
    (acr_login_server, context.acr_username, acr_password, storage_account_name,
     storage_account_key) = create_action_with_prompt_if_defined(
        context,
        'ACR and storage',
        acr_values_to_check,
        az_create_storage_and_acr,
        acr_args
    )

    env_verb = 'export' if context.os_is_linux() else 'set'
    env_statements = []
    if not local_only:
        if kubernetes:
            k8s_values_to_check = OrderedDict([
                    ('Kubernetes Cluster Name', KubernetesOperations.get_cluster_name(context))
                ])
            k8s_args = [context, root_name, resource_group, acr_login_server,
                        acr_password, ssh_public_key, ssh_private_key_path,
                        service_principal_app_id, service_principal_password]
            k8s_configured = create_action_with_prompt_if_defined(
                context,
                'Kubernetes Cluster',
                k8s_values_to_check,
                setup_k8s,
                k8s_args)
            if k8s_configured is True:
                env_statements.append('{} AML_ACS_IS_K8S=True'.format(env_verb))
        else:
            mesos_values_to_check = OrderedDict([
                ('ACS Master URL', context.acs_master_url),
                ('ACS Agent URL', context.acs_agent_url)]
            )
            mesos_args = [root_name, resource_group, acr_login_server,
                          context.acr_username, acr_password, ssh_public_key]
            create_action_with_prompt_if_defined(
                context,
                'ACS',
                mesos_values_to_check,
                az_create_acs,
                mesos_args
            )

    if isinstance(app_insights_deployment_id, types.GeneratorType):
        env_statements += ["{} AML_APP_INSIGHTS_NAME={}".format(env_verb, next(app_insights_deployment_id)),
                          "{} AML_APP_INSIGHTS_KEY={}".format(env_verb, next(app_insights_deployment_id))]

    else:
        completed_deployment = None
        while not completed_deployment:
            try:
                print('Querying App Insights deployment...')
                completed_deployment = query_deployment_status(resource_group, app_insights_deployment_id)
                time.sleep(5)
            except AzureCliError as exc:
                print(exc.message)
                break
        if completed_deployment:
            app_insights_account_name, app_insights_account_key = az_get_app_insights_account(completed_deployment)
            env_statements += ["{} AML_APP_INSIGHTS_NAME={}".format(env_verb, app_insights_account_name),
                      "{} AML_APP_INSIGHTS_KEY={}".format(env_verb, app_insights_account_key)]

    print('To configure az ml for local use with this environment, set the following environment variables.')

    env_statements += ["{} AML_STORAGE_ACCT_NAME={}".format(env_verb, storage_account_name),
                       "{} AML_STORAGE_ACCT_KEY={}".format(env_verb, storage_account_key),
                       "{} AML_ACR_HOME={}".format(env_verb, acr_login_server),
                       "{} AML_ACR_USER={}".format(env_verb, context.acr_username),
                       "{} AML_ACR_PW={}".format(env_verb, acr_password)]
    print('\n'.join([' {}'.format(statement) for statement in env_statements]))

    try:
        with open(os.path.expanduser('~/.amlenvrc'), 'w+') as env_file:
            env_file.write('\n'.join(env_statements) + '\n')
        print('You can also find these settings saved in {}'.format(os.path.join(os.path.expanduser('~'), '.amlenvrc')))
    except IOError:
        pass

    print('')


def create_action_with_prompt_if_defined(context, action_str, env_dict, action, action_args):
    prompt = True
    for key in env_dict:
        if not env_dict[key]:
            prompt = False
            break
    if prompt:
        print('Found existing {} set up.'.format(action_str))
        for key in env_dict:
            print('{0:30}: {1}'.format(key, env_dict[key]))
        answer = context.get_input('Set up a new {} instead (y/N)?'.format(action_str))
        if answer != 'y' and answer != 'yes':
            print('Continuing with configured {}.'.format(action_str))
            return (env_dict[key] for key in env_dict)
        else:
            return action(*action_args)
    return action(*action_args)
