import os
import subprocess
import re

verbose = False
env_switch_regex = r'Running in (?P<mode>[^ ]*) mode'
test_location = os.path.split(os.path.realpath(__file__))[0]
relative_path_to_samples = os.path.join('..', '..', '..', '..', 'samples')
path_to_samples = os.path.join(test_location, relative_path_to_samples)


def set_verbosity(verbosity):
    global verbose
    verbose = verbosity


def is_verbose():
    return verbose


def print_if_verbose(to_print):
    if is_verbose():
        print(to_print)


REMOTE = 'cluster'
LOCAL = 'local'


def execute_cmd(cmd):
    """

    :param cmd: list or str of command args
    :return: (str, str) : (std_out, std_err)
    """
    cmd = cmd if isinstance(cmd, str) else ' '.join(cmd)
    print_if_verbose('executing `{}`'.format(cmd))
    p = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate()
    if isinstance(out, bytes):
        out = out.decode('utf-8').strip()
    if isinstance(err, bytes):
        err = err.decode('utf-8').strip()
    print_if_verbose('stdout: {}'.format(out))
    print_if_verbose('stderr: {}'.format(err))
    return out, err


def set_env_local():
    """

    :return: bool True if successful, False otherwise
    """
    print_if_verbose('setting env local')
    cmd = ['az', 'ml', 'env', LOCAL]
    out, err = execute_cmd(cmd)
    s = re.search(env_switch_regex, out)
    return s is not None and s.group('mode') == LOCAL


def set_env_remote(force=False):
    """

    :return: bool True if successful, False otherwise
    """
    print_if_verbose('setting env remote')
    cmd = ['az', 'ml', 'env', REMOTE]
    if force:
        cmd.append('-f')
    out, err = execute_cmd(cmd)
    print_if_verbose(out)
    s = re.search(env_switch_regex, out)
    return s is not None and s.group('mode') == REMOTE

env_name_to_func = {REMOTE: set_env_remote, LOCAL: set_env_local}


def aml_env():
    cmd = ['az', 'ml', 'env']
    out, err = execute_cmd(cmd)
