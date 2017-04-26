# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

#pylint: disable=line-too-long

from azure.cli.core.commands import cli_command
from ._aml_help_formatter import AmlHelpFormatter

# batch commands
cli_command(__name__, 'ml service create batch', 'azure.cli.command_modules.ml.service.batch#batch_service_create')
cli_command(__name__, 'ml service run batch', 'azure.cli.command_modules.ml.service.batch#batch_service_run')
cli_command(__name__, 'ml service list batch', 'azure.cli.command_modules.ml.service.batch#batch_service_list')
cli_command(__name__, 'ml service view batch', 'azure.cli.command_modules.ml.service.batch#batch_service_view')
cli_command(__name__, 'ml service delete batch', 'azure.cli.command_modules.ml.service.batch#batch_service_delete')
cli_command(__name__, 'ml service viewjob batch', 'azure.cli.command_modules.ml.service.batch#batch_view_job')
cli_command(__name__, 'ml service listjobs batch', 'azure.cli.command_modules.ml.service.batch#batch_list_jobs')
cli_command(__name__, 'ml service canceljob batch', 'azure.cli.command_modules.ml.service.batch#batch_cancel_job')

# env commands
cli_command(__name__, 'ml env about', 'azure.cli.command_modules.ml.env#env_about')
cli_command(__name__, 'ml env cluster', 'azure.cli.command_modules.ml.env#env_cluster')
cli_command(__name__, 'ml env show', 'azure.cli.command_modules.ml.env#env_describe')
cli_command(__name__, 'ml env local', 'azure.cli.command_modules.ml.env#env_local')
cli_command(__name__, 'ml env setup', 'azure.cli.command_modules.ml.env#env_setup')

# realtime commands
cli_command(__name__, 'ml service create realtime', 'azure.cli.command_modules.ml.service.realtime#realtime_service_create')
cli_command(__name__, 'ml service list realtime', 'azure.cli.command_modules.ml.service.realtime#realtime_service_list')
cli_command(__name__, 'ml service view realtime', 'azure.cli.command_modules.ml.service.realtime#realtime_service_view')
cli_command(__name__, 'ml service delete realtime', 'azure.cli.command_modules.ml.service.realtime#realtime_service_delete')
cli_command(__name__, 'ml service run realtime', 'azure.cli.command_modules.ml.service.realtime#realtime_service_run')
cli_command(__name__, 'ml service scale realtime', 'azure.cli.command_modules.ml.service.realtime#realtime_service_scale')
