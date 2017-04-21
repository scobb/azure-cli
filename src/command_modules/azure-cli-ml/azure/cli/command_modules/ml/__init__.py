# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

import os.path
from pkg_resources import get_distribution, DistributionNotFound
import azure.cli.command_modules.ml._help #pylint: disable=unused-import


def load_params(_):
    import azure.cli.command_modules.ml._params #pylint: disable=redefined-outer-name


def load_commands():
    import azure.cli.command_modules.ml.commands #pylint: disable=redefined-outer-name

try:
    _dist = get_distribution('azure-cli-ml')
    # Normalize case for Windows systems
    dist_loc = os.path.normcase(_dist.location)
    here = os.path.normcase(__file__)
    if not here.startswith(dist_loc):
        # not installed, but there is another version that *is*
        raise DistributionNotFound
except DistributionNotFound:
    __version__ = 'Please install this project with setup.py'
else:
    __version__ = _dist.version