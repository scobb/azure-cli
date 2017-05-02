#!/usr/bin/env python

# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

from codecs import open
from setuptools import setup

VERSION = '0.1.0a3'

CLASSIFIERS = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5'
]

DEPENDENCIES = [
    'adal>=0.4.5',
    'azure-cli-core',
    'azure-graphrbac>=0.30.0rc6',
    'azure-mgmt-compute>=1.0.0rc1',
    'azure-mgmt-containerregistry>=0.2.0',
    'azure-mgmt-resource>=0.30.2',
    'azure-mgmt-storage>=1.0.0rc1',
    'azure-storage>=0.33',
    'future',
    'kubernetes>=1.0.0',
    'paramiko',
    'pyyaml',
    'scp',
    'tabulate>=0.7.7',
]

with open('README.rst', 'r', encoding='utf-8') as f:
    README = f.read()
with open('HISTORY.rst', 'r', encoding='utf-8') as f:
    HISTORY = f.read()

setup(
    name='azure-cli-ml',
    version=VERSION,
    description='Microsoft Azure Command-Line Tools AzureML Command Module',
    long_description=README + '\n\n' + HISTORY,
    license='MIT',
    author='Microsoft Corporation',
    author_email='azpycli@microsoft.com',
    url='https://github.com/Azure/azure-cli',
    classifiers=CLASSIFIERS,
    namespace_packages=[
        'azure',
        'azure.cli',
        'azure.cli.command_modules',
    ],
    packages=[
        'azure.cli.command_modules.ml'
    ],
    package_data={
        '': ['data/*', 'service/data/*', 'service/*']
    },
    install_requires=DEPENDENCIES,
)
