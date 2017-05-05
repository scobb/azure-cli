import os
from azure.cli.command_modules.ml._util import CommandLineInterfaceContext


class E2eContext(CommandLineInterfaceContext):
    ssh_private_key_path = None

    def __init__(self, name):
        super(E2eContext, self).__init__()
        self.name = name
        self.local_mode = False

    def in_local_mode(self):
        return self.local_mode

    def name(self):
        return self.name

    @staticmethod
    def get_acs_ssh_private_key_path():
        return E2eContext.ssh_private_key_path
