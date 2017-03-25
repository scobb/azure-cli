import requests
import subprocess
import socket
from ._util import CommandLineInterfaceContext
from ._util import acs_connection_timeout
from ._util import InvalidConfError
from .service._realtimeutilities import check_marathon_port_forwarding


def acs_marathon_setup(context):
    """Helps set up port forwarding to an ACS cluster."""

    if context.os_is_linux():
        print('Establishing connection to ACS cluster.')
        acs_url = context.get_input(
            'Enter ACS Master URL (default: {}): '.format(context.acs_master_url))
        if acs_url is None or acs_url == '':
            acs_url = context.acs_master_url
            if acs_url is None or acs_url == '':
                print('Error: no ACS URL provided.')
                return False, -1

        acs_username = context.get_input('Enter ACS username (default: acsadmin): ')
        if acs_username is None or acs_username == '':
            acs_username = 'acsadmin'

        # Find a random unbound port
        sock = context.get_socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        local_port = sock.getsockname()[1]
        print('Forwarding local port {} to port 80 on your ACS cluster'.format(
            local_port))
        try:
            sock.close()
            context.check_call(['ssh', '-L', '{}:localhost:80'.format(local_port),
                                '-f', '-N', '{}@{}'.format(acs_username, acs_url), '-p',
                                '2200'])
            return True, local_port
        except subprocess.CalledProcessError as ex:
            print('Failed to set up ssh tunnel. Error code: {}'.format(ex.returncode))
            return False, -1
    print('Unable to automatically set port forwarding for Windows machines.')
    return False, -1


def validate_acs_marathon(context, existing_port):
    """

    Tests whether a valid connection to an ACS cluster exists.
    :param existing_port: If -1, check for an existing configuration setting indicating port forwarding in ~/.amlconf.
                          If 0, check for a direct connection to the ACS cluster specified in $AML_ACS_MASTER.
                          If > 0, check for port forwarding to the specified port.
    :return: (bool,int) - First value indicates whether a successful connection was made. Second value indicates the
                          port on which the connection was made. 0 indicates direct connection. Any other positive
                          integer indicates port forwarding is ON to that port.
    """
    if existing_port < 0:
        existing_port = check_marathon_port_forwarding(context)

    # port forwarding was previously setup, verify that it still works
    if existing_port > 0:
        marathon_base_url = 'http://127.0.0.1:' + str(existing_port) + '/marathon/v2'
        marathon_info_url = marathon_base_url + '/info'

        try:
            marathon_info = context.http_call('get', marathon_info_url,
                                              timeout=acs_connection_timeout)
        except (requests.ConnectionError, requests.exceptions.ReadTimeout):
            print('Marathon endpoint not available at {}'.format(marathon_base_url))
            config_port = check_marathon_port_forwarding(context)
            if config_port == 0:
                print(
                    'Found previous direct connection to ACS cluster. Checking if it still works.')
                return validate_acs_marathon(context, config_port)
            elif config_port > 0 and config_port != existing_port:
                print(
                    'Found previous port forwarding set up at {}. Checking if it still works.'.format(
                        config_port))
                return validate_acs_marathon(context, config_port)
            return acs_marathon_setup(context)
        try:
            marathon_info = marathon_info.json()
        except ValueError:
            print('Marathon endpoint not available at {}'.format(marathon_base_url))
            return acs_marathon_setup(context)
        if 'name' in marathon_info and 'version' in marathon_info and marathon_info[
            'name'] == 'marathon':
            print(
                'Successfully tested ACS connection. Found marathon endpoint at {}'.format(
                    marathon_base_url))
            return (True, existing_port)
        else:
            print('Marathon endpoint not available at {}'.format(marathon_base_url))
            return acs_marathon_setup(context)

    # direct connection was previously setup, or is being requested, verify that it works
    elif existing_port == 0:
        if context.acs_master_url is not None and context.acs_master_url != '':
            marathon_base_url = 'http://' + context.acs_master_url + '/marathon/v2'
            print(
                'Trying direct connection to ACS cluster at {}'.format(marathon_base_url))
            marathon_info_url = marathon_base_url + '/info'
            try:
                marathon_info = context.http_call('get', marathon_info_url,
                                                  timeout=acs_connection_timeout)
            except (requests.ConnectionError, requests.exceptions.ReadTimeout):
                print('Marathon endpoint not available at {}'.format(marathon_base_url))
                return (False, -1)
            try:
                marathon_info = marathon_info.json()
            except ValueError:
                print('Marathon endpoint not available at {}'.format(marathon_base_url))
                return (False, -1)
            if 'name' in marathon_info and 'version' in marathon_info and marathon_info[
                'name'] == 'marathon':
                print(
                    'Successfully tested ACS connection. Found marathon endpoint at {}'.format(
                        marathon_base_url))
                return (True, 0)
            else:
                print('Marathon endpoint not available at {}'.format(marathon_base_url))
                return (False, -1)
        else:
            return (False, -1)

    # No connection previously setup
    else:
        # Try ssh tunnel first
        (forwarding_set, port) = acs_marathon_setup(context)
        if not forwarding_set:
            # Try direct connection
            return validate_acs_marathon(context, 0)
        else:
            return (forwarding_set, port)


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
    AML_APP_INSIGHTS_NAME : Set this to an App Insights account
    AML_APP_INSIGHTS_KEY  : Set this to an App Insights instrumentation key


    Cluster mode:
    To enter cluster mode: az ml env cluster

    In cluster mode, the CLI can be used to deploy production web services. Realtime web services are deployed to
    an Azure Container Service (ACS) cluster, and batch web services are deployed to an HDInsight Spark cluster. In
    order to use the CLI in cluster mode, define the following environment variables (in addition to those above for
    local mode):

    AML_ACS_MASTER        : Set this to the URL of your ACS Master (e.g.yourclustermgmt.westus.cloudapp.azure.com)
    AML_ACS_AGENT         : Set this to the URL of your ACS Public Agent (e.g. yourclusteragents.westus.cloudapp.azure.com)
    AML_HDI_CLUSTER       : Set this to the URL of your HDInsight Spark cluster.
    AML_HDI_USER          : Set this to the admin user of your HDInsight Spark cluster.
    AML_HDI_PW            : Set this to the password of the admin user of your HDInsight Spark cluster.
    """)


def env_cluster(force_connection, forwarded_port, verb, context=CommandLineInterfaceContext()):
    """Switches environment to cluster mode."""

    if force_connection and forwarded_port != -1:
        print('Unable to force direct connection when -p is specified.')
        print('Please use -f and -p exclusively.')
        return

    # if -f was specified, try direct connection only
    if force_connection:
        (acs_is_setup, port) = validate_acs_marathon(context, 0)
    # if only -p specified, without a port number, set up a new tunnel.
    elif not forwarded_port:
        (acs_is_setup, port) = acs_marathon_setup(context)
    # if either no arguments specified (forwarded_port == -1), or -p NNNNN specified (forwarded_port == NNNNN),
    # test for an existing connection (-1), or the specified port (NNNNN)
    elif forwarded_port:
        (acs_is_setup, port) = validate_acs_marathon(context, forwarded_port)
    # This should never happen
    else:
        (acs_is_setup, port) = (False, -1)

    if not acs_is_setup:
        continue_without_acs = context.get_input(
            'Could not connect to ACS cluster. Continue with cluster mode anyway (y/N)? ')
        continue_without_acs = continue_without_acs.strip().lower()
        if continue_without_acs != 'y' and continue_without_acs != 'yes':
            print(
                "Aborting switch to cluster mode. Please run 'az ml env about' for more information on setting up your cluster.")  # pylint: disable=line-too-long
            return

    try:
        conf = context.read_config()
        if not conf:
            conf = {}
    except InvalidConfError:
        if verb:
            print('[Debug] Suspicious content in ~/.amlconf.')
            print('[Debug] Resetting.')
        conf = {}

    conf['mode'] = 'cluster'
    conf['port'] = port
    context.write_config(conf)

    print("Running in cluster mode.")
    env_describe(context)


def env_describe(context=CommandLineInterfaceContext()):
    """Print current environment settings."""
    # TODO - update with app insights
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

    if not context.in_local_mode():
        print('HDI cluster URL        : {}'.format(context.hdi_home))
        print('HDI admin user name    : {}'.format(context.hdi_user))
        print('HDI admin password     : {}'.format(context.hdi_pw))
        print('ACS Master URL         : {}'.format(context.acs_master_url))
        print('ACS Agent URL          : {}'.format(context.acs_agent_url))
        forwarded_port = check_marathon_port_forwarding(context)
        if forwarded_port > 0:
            print('ACS Port forwarding    : ON, port {}'.format(forwarded_port))
        else:
            print('ACS Port forwarding    : OFF')


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
