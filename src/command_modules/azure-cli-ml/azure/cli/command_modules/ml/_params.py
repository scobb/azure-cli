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

# batch workflows
register_cli_argument('ml service', 'service_name', options_list='-n', help='Webservice name.')
register_cli_argument('ml service', 'job_name', options_list='-j', help='Job name.')
register_cli_argument('ml service', 'verb', options_list='-v', required=False, help='Verbosity flag.', action='store_true')
register_cli_argument('ml service create batch', 'driver_file', options_list=('-f', '--driver-file'))
# register_cli_argument('ml service', 'outputs', options_list=('--out'), required=False, action='append', default=[])
# register_cli_argument('ml service', 'inputs', options_list=('-i', '--in'), required=False, action='append', default=[])
# register_cli_argument('ml service', 'parameters', options_list=('-p', '--param'), required=False, action='append', default=[])
# register_cli_argument('ml service', 'dependencies', options_list=('-d', '--dep'), required=False, action='append', default=[])
register_cli_argument('ml service create batch', 'title', required=False)

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
                        help='Files and directories required by the service. Multiple dependencies can be specified with additional -d arguments.', required=False)
register_cli_argument('ml service create realtime', 'score_file', options_list='-f', metavar='filename',
                      help='The code file to be deployed.')
register_cli_argument('ml service create realtime', 'requirements', options_list='-p',
                      metavar='requirements.txt', default='', help='A pip requirements.txt file of packages needed by the code file.', required=False)
register_cli_argument('ml service create realtime', 'model', options_list='-m',
                      default='', help='The model to be deployed.', required=False)
# TODO: Add documentation about schema file format
register_cli_argument('ml service create realtime', 'schema_file', options_list='-s', default='', required=False,
                      help='Input and output schema of the web service.')
register_cli_argument('ml service create realtime', 'custom_ice_url', options_list='-i', default='', required=False,
                      help=argparse.SUPPRESS)
register_cli_argument('ml service create realtime', 'target_runtime', options_list='-r', default='spark-py',
                      help='Runtime of the web service. Valid runtimes are {}'.format('|'.join(RealtimeConstants.supported_runtimes)), required=False)
register_cli_argument('ml service create realtime', 'logging_level', options_list='-l', default='none', const = 'debug',
                      nargs='?', help='Logging level. Valid levels are {}'.format('|'.join(RealtimeConstants.supported_logging_levels)), required=False)

