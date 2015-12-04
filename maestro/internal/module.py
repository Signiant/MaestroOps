"""
module.py
    
Modules are one-off runnable tasks. All modules should extend the Module() class.

Modules are meant to help build modular tools. 
"""
import logging, types, copy_reg
from multiprocessing import Process, log_to_stderr
from multiprocessing.pool import Pool
from logging import INFO

#Status states
NOT_STARTED = 0
RUNNING = 1
DONE = 2

class NoDaemonProcess(Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

class NonDaemonizedPool(Pool):
    Process = NoDaemonProcess

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

    def help(self):
        try:
            print HELPTEXT
        except NameError:
            print "No help defined for module " + str(type(self))

def __pickle_method__(m):
    """
    Pickling override for using pickle with instance methods.
    """
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
        """
        Sets the current status and calls the superclass' init
        """
        super(AsyncModule,self).__init__(ioc)
        self.status = NOT_STARTED

    def start(self,kwargs={}):
        """
        The main method to start a module. In Async, it will return immediately with the result from apply_async. 
        """
        pool = NonDaemonizedPool(processes=1)
	self.status = RUNNING
	result = pool.apply_async(self.__setup__,[kwargs],callback=self.__finish_internal__)
        return result

    def __setup__(self,kwargs):
        """
        Internal setup method, this should not be overridden unless you know what you're doing. Calls the run method.
        """
        #Set up logger
        #self.__logger__ = log_to_stderr()
        #self.__logger__.setLevel(INFO)
        return self.run(kwargs=kwargs)

    def __finish_internal__(self,callback_args):
        """
        Internal finish method. This should not be overridden unless you know what you're doing. Calls the finish method.
        """
        self.status = DONE
        self.finish(callback_args)

    def finish(self, results):
        """
        Finish method, overridable but otherwise sticks the results into a variable, or sets the exception.
        """
        if isinstance(results,Exception):
            self.exception = results
        else:
            self.result = results

    def log(self, message, level=INFO):
        """
        Log method, currently set to only print to the console. Thread-safe.
        """
        self.__logger__.info(message)
