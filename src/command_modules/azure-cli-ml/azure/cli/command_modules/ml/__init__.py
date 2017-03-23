# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

import azure.cli.command_modules.ml._help #pylint: disable=unused-import


def load_params(_):
    import azure.cli.command_modules.ml._params #pylint: disable=redefined-outer-name
    from azure.cli.core.commands import APPLICATION
    from ._aml_help_formatter import AmlHelpFormatter
    APPLICATION.parser.formatter_class = AmlHelpFormatter
    for parser_key in APPLICATION.parser.subparsers:
        print '{}: {}'.format(parser_key, APPLICATION.parser.subparsers[parser_key])
        subparser = APPLICATION.parser.subparsers[parser_key]
        for choice_key in subparser.choices:
            choice = subparser.choices[choice_key]
            choice.formatter_class = AmlHelpFormatter
        # if 'batch' in subparser.choices:
        #     print('Making it an AmlHelpFormatter')
        #     subparser.choices['batch'].formatter_class = AmlHelpFormatter
        #     print subparser.choices['batch']

def load_commands():
    import azure.cli.command_modules.ml.commands #pylint: disable=redefined-outer-name
    # from azure.cli.core.commands import APPLICATION
    # from ._aml_help_formatter import AmlHelpFormatter
    # APPLICATION.global_parser.formatter_class = AmlHelpFormatter
    # APPLICATION.parser.formatter_class = AmlHelpFormatter
