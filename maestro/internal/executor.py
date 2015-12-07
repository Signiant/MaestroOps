import ioc
from module import Module

def Executor(object):
    
    def entry(self):
        pass

def ModuleExecutor(Module):
    
    kwargs = None
    args = None

    def __init__(self):
        pass


    def entry(self):
        kwargs, args = self.parse_sysargs
        self.run(None)

    def parse_sysargs(self):
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

        return kwargs, args

if __name__ == "__main__":
    ex = ModuleExecutor()
