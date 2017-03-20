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
register_cli_argument('aml service batch', 'service_name', options_list='-n', help='Webservice name.')
register_cli_argument('aml service batch', 'job_name', options_list='-j', help='Job name.')
register_cli_argument('aml service batch', 'verb', options_list='-v', required=False, help='Verbosity flag.')
