# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
Utilities to work with docker.

"""


import base64
import json
import os


def check_docker_credentials(acr_home, acr_user, acr_pw, verbose):
    """
    Create a ~/.docker/config.json file if none exists.
    Ensure that the config.json file has credentials for the ACR passed in.
    :param acr_home: The login server URL of the ACR
    :param acr_user: The username to access the ACR
    :param acr_pw: The password for the above username
    :param verbose: Whether to print verbose output or not
    :returns None
    """
    if not os.path.exists(os.path.expanduser('~/.docker/config.json')):
        if verbose:
            print('Docker config not found. Creating new config file with ACR credentials.')
        if not os.path.exists(os.path.expanduser('~/.docker')):
            os.mkdir(os.path.expanduser('~/.docker'))

        add_docker_credentials(acr_home, acr_user, acr_pw, verbose)
        return
    else:
        try:
            with open(os.path.expanduser('~/.docker/config.json'), 'r') as docker_config_file:
                docker_config = docker_config_file.read()
        except IOError:
            print('Error configuring docker for your ACR. Please try the following:')
            print('docker login {}'.format(acr_home))
            return
        try:
            docker_config = json.loads(docker_config)
        except ValueError:
            print('Your docker configuration file is corrupt. Please try the following:')
            print('docker login {}'.format(acr_home))
            return
        if 'auths' in docker_config and acr_home in docker_config['auths']:
            return
        else:
            connection_string = base64.b64encode(bytes(acr_user + ':' + acr_pw, 'utf-8'))
            docker_auth = {'auth': connection_string.decode('ascii')}
            docker_config['auths'][acr_home] = docker_auth
            if verbose:
                print(json.dumps(docker_config))
            with open(os.path.expanduser('~/.docker/config.json'), 'w+') as dockerconfig_file:
                dockerconfig_file.write(json.dumps(docker_config))


def add_docker_credentials(acr_home, acr_user, acr_pw, verbose):
    """
    Adds the given credentials to the docker config file.
    :param acr_home: The login server URL of the ACR
    :param acr_user: The username to access the ACR
    :param acr_pw: The password for the above username
    :param verbose: Whether to print verbose output or not
    :returns None
    """
    connection_string = base64.b64encode(bytes(acr_user + ':' + acr_pw, 'utf-8'))
    docker_auths = {acr_home: {'auth': connection_string.decode('ascii')}}
    docker_config = {'auths': docker_auths}
    if verbose:
        print(json.dumps(docker_config))
    with open(os.path.expanduser('~/.docker/config.json'), 'w+') as dockerconfig_file:
        dockerconfig_file.write(json.dumps(docker_config))
