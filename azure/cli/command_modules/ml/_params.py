# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

# pylint: disable=line-too-long
import argparse

from azure.cli.core.commands import register_cli_argument
from azure.cli.core.commands.parameters import ignore_type
from .service._realtimeutilities import RealtimeConstants

# ignore the context--not for users
register_cli_argument('', 'context', arg_type=ignore_type)

# used throughout
register_cli_argument('ml', 'verb', options_list=('-v',), required=False, help='Verbosity flag.', action='store_true')

register_cli_argument('ml service', 'service_name', options_list=('-n',), help='Webservice name.')
register_cli_argument('ml service', 'job_name', options_list=('-j',), help='Job name.')
register_cli_argument('ml service', 'dependencies', options_list=('-d',), action='append',
                      metavar='<dependency> [-d...]', default=[],
                        help='Files and directories required by the service. Multiple dependencies can be specified with additional -d arguments.', required=False)

# batch workflows
register_cli_argument('ml service create batch', 'driver_file', options_list=('-f', '--driver-file'))
register_cli_argument('ml service create batch', 'title', required=False)
register_cli_argument('ml service run batch', 'job_name', required=False, options_list=('-j',), help='Job name. Defaults to a formatted timestamp (%Y-%m-%d_%H%M%S)')
register_cli_argument('ml service run batch', 'wait_for_completion', required=False, options_list=('-w',), action='store_true', help='Flag to wait for job synchronously.')
register_cli_argument('ml service', 'inputs', options_list=('--in',), action='append',
                      metavar='<input_name>[:<default_value>] [--in=...]',
                      help='inputs for service to expect', default=[], required=False)
register_cli_argument('ml service', 'outputs', options_list=('--out',), action='append',
                        metavar='<output_name>[:<default_value>] [--out=...]', default=[],
                        help='outputs for service to expect', required=False)
register_cli_argument('ml service', 'parameters', options_list=('--param',), action='append',
                        metavar='<parameter_name>[:<default_value>] [--param=...]', default=[],
                        help='parameters for service to expect', required=False)

# realtime workflows
register_cli_argument('ml service create realtime', 'score_file', options_list=('-f',), metavar='filename',
                      help='The code file to be deployed.')
register_cli_argument('ml service create realtime', 'requirements', options_list=('-p',),
                      metavar='requirements.txt', default='', help='A pip requirements.txt file of packages needed by the code file.', required=False)
register_cli_argument('ml service create realtime', 'model', options_list=('-m',),
                      default='', help='The model to be deployed.', required=False)
# TODO: Add documentation about schema file format
register_cli_argument('ml service create realtime', 'schema_file', options_list=('-s',), default='', required=False,
                      help='Input and output schema of the web service.')
register_cli_argument('ml service create realtime', 'custom_ice_url', options_list=('-i',), default='', required=False,
                      help=argparse.SUPPRESS)
register_cli_argument('ml service create realtime', 'target_runtime', options_list=('-r',), default='spark-py',
                      help='Runtime of the web service. Valid runtimes are {}'.format('|'.join(RealtimeConstants.supported_runtimes)), required=False)
register_cli_argument('ml service create realtime', 'app_insights_logging_enabled', options_list=('-l',), action='store_true',
                      help='Flag to enable App insights logging.', required=False)
register_cli_argument('ml service run realtime', 'input_data', options_list=('-d',), default='',
                      help='The data to use for calling the web service.', required=False)
register_cli_argument('ml service create realtime', 'num_replicas', options_list=('-z',),
                      default=1, required=False, help='Number of replicas for a Kubernetes service.')
register_cli_argument('ml service scale realtime', 'num_replicas', options_list=('-z',),
                      default=1, required=True, help='Number of replicas for a Kubernetes service.')

# env workflows
register_cli_argument('ml env cluster', 'force_connection', options_list=('-f',), action='store_true',
                       help='Force direct connection to ACS cluster.',
                      required=False)
register_cli_argument('ml env cluster', 'forwarded_port', options_list=('-p',), nargs='?',
                      const=None, default=-1, type=int, required=False,
                      help='Use port forwarding. If a port number is specified, test for an existing tunnel. Without a port number, try to set up an ssh tunnel through an unused port.' #pylint: disable=line-too-long
                      )
register_cli_argument('ml env setup', 'status', options_list=('-s', '--status'), metavar='deploymentId', help='Check the status of an ongoing deployment.', required=False)
register_cli_argument('ml env setup', 'name', options_list=('-n', '--name'), metavar='envName', help='The name of your Azure ML environment (1-20 characters, alphanumeric only).', required=False)
register_cli_argument('ml env setup', 'kubernetes', options_list=('-k', '--kubernetes'), action='store_true', help='Sets up a new Kubernetes cluster.', required=False)
register_cli_argument('ml env setup', 'local_only', options_list=('-l', '--local-only'), action='store_true', help='Sets up only local mode (no ACS).', required=False)
register_cli_argument('ml env setup', 'service_principal_app_id', options_list=('-a', '--service-principal-app-id'), help='App ID of service principal to use for configuring ACS cluster.', required=False)
register_cli_argument('ml env setup', 'service_principal_password', options_list=('-p', '--service-principal-password'), help='Password associated with service principal.', required=False)
