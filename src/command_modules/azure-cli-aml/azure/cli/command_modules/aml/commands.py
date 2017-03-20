# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

#pylint: disable=line-too-long

from azure.cli.core.commands import cli_command

cli_command(__name__, 'aml service batch list', 'azure.cli.command_modules.aml.service.batch#batch_service_list')
cli_command(__name__, 'aml service batch view', 'azure.cli.command_modules.aml.service.batch#batch_service_view')
cli_command(__name__, 'aml service batch delete', 'azure.cli.command_modules.aml.service.batch#batch_service_delete')
cli_command(__name__, 'aml service batch viewjob', 'azure.cli.command_modules.aml.service.batch#batch_view_job')
cli_command(__name__, 'aml service batch listjobs', 'azure.cli.command_modules.aml.service.batch#batch_list_jobs')
cli_command(__name__, 'aml service batch canceljob', 'azure.cli.command_modules.aml.service.batch#batch_cancel_job')