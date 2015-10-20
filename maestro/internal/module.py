"""
module.py
    
Modules are one-off runnable tasks. All modules should extend the Module() class.

Modules are meant to help build modular tools. 
"""

NOT_STARTED = 0
RUNNING = 1
DONE = 2
FAILED = 3

class Module(object):
"""
Base module class. All modules take an ObjectContainer on initialization, and must override the run() method 
"""
    id = ''
    __ioc = None

    def __init__(self,ioc):
        self.__ioc = ioc    

    def getModule(self,name):
        __ioc.getModule(name)
    
    def run(self,kwargs={}):
        return None


class AsyncModule(Module):
"""
Builds on the Module class to add asynchronous functionality for modules that can support it.
"""
    status = None
    result = None
    exception = None

    def __init__(self,ioc):
        super(AsyncModule,self).__init__(ioc)
        self.status = NOT_STARTED

    def run_async(self,kwargs={}):
	    self.status = RUNNING
	    self.run(args)
        return None
