# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

# pylint: disable=line-too-long

from azure.cli.core.commands import register_cli_argument
from azure.cli.core.commands.parameters import ignore_type

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
                        help='dependencies required for service', required=False)
