"""
module.py
    
Modules are one-off runnable tasks. All modules should extend the Module() class.

Modules are meant to help build modular tools. 
"""
import logging, types, copy_reg
from multiprocessing import Pool, log_to_stderr
from logging import INFO

#Status states
NOT_STARTED = 0
RUNNING = 1
DONE = 2

class Module(object):
    """
    Base module class. All modules take an ObjectContainer on initialization, and must override the run() method 
    """
    id = ''
    __ioc__ = None

    def __init__(self,ioc):
        self.__ioc__ = ioc    

    def getModule(self,name):
        self.__ioc__.getModule(name)
    
    def run(self,kwargs={}):
        pass

    def start(self,kwargs={}):
        self.run(kwargs=kwargs)

def __pickle_method__(m):
        if m.im_self is None:
            return getattr, (m.im_class, m.im_func.func_name)
        else:
            return getattr, (m.im_self, m.im_func.func_name)

copy_reg.pickle(types.MethodType, __pickle_method__)

class AsyncModule(Module):
    """
    Builds on the Module class to add asynchronous functionality for modules that can support it. 

    Intra/inter synchronization must be handled by each module. Modules are seperate processes.
    """
    status = None
    result = None
    exception = None
    __logger__ = None

    def __init__(self,ioc):
        super(AsyncModule,self).__init__(ioc)
        self.status = NOT_STARTED

    def start(self,kwargs={}):
        pool = Pool(processes=1)
	self.status = RUNNING
	result = pool.apply_async(self.__setup__,[kwargs],callback=self.__finish_internal__)
        return result

    def __setup__(self,kwargs):
        self.__logger__ = log_to_stderr()
        self.__logger__.setLevel(INFO)
        self.__logger__.info("Beginning..." + str(kwargs))
        return self.run(kwargs=kwargs)

    def __finish_internal__(self,callback_args):
        self.status = DONE
        self.finish(callback_args)

    def finish(self, results):
        if isinstance(results,Exception):
            self.exception = results
        else:
            self.result = results

    def log(self, message, level=INFO):
        self.__logger__.info(message)
