from ioc import SingleObjectContainer
from module import Module
import sys

class ModuleExecuter(Module):
    """
    Simple executer which parses kwargs and args. Passes kwargs into itself, and calls "start"
    """
    kwargs = None
    args = None
    exit_code = 0

    def __init__(self):
        self.entry()

    def register(self, module):
        if self.__ioc__ is None:
            self.__ioc__ = SingleObjectContainer(obj_type = Module)
        module.__ioc__ = self.__ioc__
        self.__ioc__.register(module.id, module)

    #Entry point for the executer
    def entry(self):
        self.kwargs, self.args = parse_sysargs()
        self.start(kwargs = self.kwargs)
        
def parse_sysargs():
    current_key = None
    kwargs = dict()
    args = list()
    try:
        for arg in sys.argv[1:]:
            if current_key is None:
                if arg.startswith('-'):
                    current_key = arg.lstrip('-')
                else:
                    args.append(arg)
                continue
            if current_key is not None and not arg.startswith("-"):
                kwargs[current_key] = arg
                current_key = None
            else:
                kwargs[current_key] = None
                current_key = arg.lstrip('-')
        if current_key is not None and current_key not in kwargs:
            kwargs[current_key] = None
            current_key = None
    except IndexError:
        args.append(sys.argv[1])

    return kwargs, args

