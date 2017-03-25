from .._util import InvalidConfError
from .._util import is_int


def check_marathon_port_forwarding(context):
    """

    Check if port forwarding is set up to the ACS master
    :return: int - -1 if config error, 0 if direct cluster connection is set up, local port otherwise
    """
    try:
        conf = context.read_config()
        if not conf:
            return -1
    except InvalidConfError:
        return -1

    if 'port' in conf and is_int(conf['port']):
        return int(conf['port'])

    return -1
