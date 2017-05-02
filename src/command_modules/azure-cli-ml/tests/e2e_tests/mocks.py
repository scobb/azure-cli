import os
from azure.cli.command_modules.ml._util import CommandLineInterfaceContext


class E2eContext(CommandLineInterfaceContext):
    def __init__(self):
        super(E2eContext, self).__init__()
        self.local_mode = bool(os.environ.get('AML_LOCAL_MODE', 'False'))
        self.forwarded_port = -1

    def in_local_mode(self):
        return self.local_mode

    def check_marathon_port_forwarding(self):
        return self.forwarded_port
