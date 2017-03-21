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
register_cli_argument('ml service batch', 'service_name', options_list='-n', help='Webservice name.')
register_cli_argument('ml service batch', 'job_name', options_list='-j', help='Job name.')
register_cli_argument('ml service batch', 'verb', options_list='-v', required=False, help='Verbosity flag.', action='store_true')
register_cli_argument('ml service batch create', 'driver_file', options_list=('-f', '--driver-file'), action='append', default=[])
register_cli_argument('ml service batch', 'outputs', options_list=('--out'), required=False, action='append', default=[])
register_cli_argument('ml service batch', 'inputs', options_list=('-i', '--in'), required=False, action='append', default=[])
register_cli_argument('ml service batch', 'parameters', options_list=('-p', '--param'), required=False, action='append', default=[])
register_cli_argument('ml service batch', 'dependencies', options_list=('-d', '--dep'), required=False, action='append', default=[])
register_cli_argument('ml service batch', 'title', required=False)

# register_cli_argument('ml service batch', 'inputs', options_list='--input', action='append', metavar='<input_name>:[<default_value>] [--input=...]', help='inputs for service to expect', default=[])
# register_cli_argument('ml service batch', 'outputs', options_list='--output', action='append',
#                         metavar='<output_name>:[<default_value>] [--output=...]', default=[],
#                         help='outputs for service to expect')
# register_cli_argument('ml service batch', 'parameters', options_list='--parameter', action='append',
#                         metavar='<parameter_name>:[<default_value>] [--parameter=...]', default=[],
#                         help='parameters for service to expect')
# register_cli_argument('ml service batch', 'dependencies', options_list='--dependency', action='append', metavar='<dependency> [-d...]', default=[],
#                         help='dependencies required for service')