import os
from azure.cli.command_modules.ml._util import CommandLineInterfaceContext


class E2eContext(CommandLineInterfaceContext):
    def __init__(self, name):
        super(E2eContext, self).__init__()
        self.name = name
        self.local_mode = False

    def in_local_mode(self):
        return self.local_mode

    def name(self):
        return self.name
