from azure.cli.command_modules.ml._util import CommandLineInterfaceContext


class E2eContext(CommandLineInterfaceContext):
    def __init__(self):
        super(E2eContext, self).__init__()
        self.local_mode = False

    def in_local_mode(self):
        return self.local_mode
