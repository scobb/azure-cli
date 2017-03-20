# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

#pylint: disable=line-too-long

from azure.cli.core.commands import cli_command


cli_command(__name__, 'aml command1', 'azure.cli.command_modules.aml.custom#example_custom')
cli_command(__name__, 'aml command2', 'azure.cli.command_modules.aml.custom#example_custom_two')
