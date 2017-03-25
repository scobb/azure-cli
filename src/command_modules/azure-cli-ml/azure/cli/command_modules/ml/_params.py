# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

# pylint: disable=line-too-long

from azure.cli.core.commands import register_cli_argument
from azure.cli.core.commands.parameters import ignore_type

# ignore the context--not for users
register_cli_argument('', 'context', arg_type=ignore_type)

# used throughout
register_cli_argument('ml', 'verb', options_list='-v', required=False, help='Verbosity flag.', action='store_true')

# batch workflows
register_cli_argument('ml service', 'service_name', options_list='-n', help='Webservice name.')
register_cli_argument('ml service', 'job_name', options_list='-j', help='Job name.')
register_cli_argument('ml service create batch', 'driver_file', options_list=('-f', '--driver-file'))
register_cli_argument('ml service create batch', 'title', required=False)
register_cli_argument('ml service run batch', 'job_name', required=False, options_list='-j', help='Job name. Defaults to a formatted timestamp (%Y-%m-%d_%H%M%S)')
register_cli_argument('ml service run batch', 'wait_for_completion', required=False, options_list='-w', action='store_true', help='Flag to wait for job synchronously.')

register_cli_argument('ml service', 'inputs', options_list='--in', action='append',
                      metavar='<input_name>:[<default_value>] [--in=...]',
                      help='inputs for service to expect', default=[], required=False)
register_cli_argument('ml service', 'outputs', options_list='--out', action='append',
                        metavar='<output_name>:[<default_value>] [--out=...]', default=[],
                        help='outputs for service to expect', required=False)
register_cli_argument('ml service', 'parameters', options_list='--param', action='append',
                        metavar='<parameter_name>:[<default_value>] [--param=...]', default=[],
                        help='parameters for service to expect', required=False)
register_cli_argument('ml service', 'dependencies', options_list='-d', action='append',
                      metavar='<dependency> [-d...]', default=[],
                        help='dependencies required for service', required=False)


# env workflows
register_cli_argument('ml env cluster', 'force_connection', options_list='-f', action='store_true',
                       help='Force direct connection to ACS cluster.',
                      required=False)
register_cli_argument('ml env cluster', 'forwarded_port', options_list='-p', nargs='?',
                      const=None, default=-1, type=int, required=False,
                      help='Use port forwarding. If a port number is specified, test for an existing tunnel. Without a port number, try to set up an ssh tunnel through an unused port.' #pylint: disable=line-too-long
                      )
register_cli_argument('ml env setup', 'status', options_list=('-s', '--status'), metavar='deploymentId', help='Check the status of an ongoing deployment.', required=False)
register_cli_argument('ml env setup', 'name', options_list=('-n', '--name'), metavar='envName', help='The name of your Azure ML environment (1-20 characters, alphanumeric only).', required=False)