"""
module.py

Modules are one-off runnable tasks. All modules should extend the Module() class.

Modules are meant to help build modular tools.
"""
import types
import copyreg
import sys
import traceback
import multiprocessing
from multiprocessing.pool import Pool
from logging import INFO

# Module states
NOT_STARTED = 0
RUNNING = 1
DONE = 2
PROCESSED = sys.maxsize


# Exceptions
class AsyncException(Exception):
    """
    Async exceptions are the exceptions that are returned from an AsyncModule. It provides the
    original traceback as a string under "traceback", and also provides the original string
    message under "message"
    """
    traceback = None


class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess


class NonDaemonizedPool(Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NonDaemonizedPool, self).__init__(*args, **kwargs)

# TODO: change modules to accept kwargs and args


class Module(object):
    """
    Base module class. All modules may take an ObjectContainer on initialization, and must override the run() method
    """
    id = ''
    __ioc__ = None

    def __init__(self, ioc=None):
        self.__ioc__ = ioc

    def getObject(self, name):
        return self.__ioc__.get(name)

    def getObjectInstance(self, name):
        return self.__ioc__.getinstance(name)

    def run(self, kwargs={}):
        pass

    def start(self, kwargs={}):
        self.run(kwargs=kwargs)

    def help(self):
        try:
            print(self.HELPTEXT)
        except NameError:
            print("No help defined for module " + str(type(self)))
        return True


def __pickle_method__(m):
    """
    Pickling override for using pickle with instance methods.
    """
    if m.__self__ is None:
        return getattr, (m.__self__.__class__, m.__func__.__name__)
    else:
        return getattr, (m.__self__, m.__func__.__name__)


copyreg.pickle(types.MethodType, __pickle_method__)


class AsyncModule(Module):
    """
    Builds on the Module class to add asynchronous functionality for modules that can support it.

    Intra/inter synchronization must be handled by each module. Modules are seperate processes.
    """
    status = None
    result = None
    exception = None
    __logger__ = None

    def __init__(self, ioc=None):
        """
        Sets the current status and calls the superclass' init
        """
        super(AsyncModule, self).__init__(ioc=ioc)
        self.status = NOT_STARTED

    def start(self, kwargs={}):
        """
        The main method to start a module. In Async, it will return immediately with the result from apply_async.
        """
        pool = NonDaemonizedPool(processes=1)
        self.status = RUNNING
        result = pool.apply_async(self.__setup__, [kwargs], callback=self.__finish_internal__)
        return result

    def __setup__(self, kwargs):
        """
        Internal setup method, this should not be overridden unless you know what you're doing. Calls the run method.
        """
        # If we're an AsyncModule, we need to catch Exceptions so they can be converted and passed to the parent process
        try:
            return self.run(kwargs=kwargs)
        except Exception as e:
            try:
                exc = AsyncException(e)
                exc.traceback = "".join(traceback.format_exception(*sys.exc_info()))
                return exc
            except Exception as e:
                print("FATAL ERROR -- " + str(e))

    def __finish_internal__(self, callback_args):
        """
        Internal finish method. This should not be overridden unless you know what you're doing. Calls the finish method.
        """
        self.status = DONE
        self.finish(callback_args)

    def finish(self, results):
        """
        Finish method, overridable but otherwise sticks the results into a variable, or sets the exception.
        """
        if isinstance(results, Exception):
            self.exception = results
        else:
            self.result = results

    def log(self, message, level=INFO):
        """
        Log method, currently set to only print to the console. Thread-safe.
        """
        if self.__logger__ is None:
            self.__logger__ = multiprocessing.log_to_stderr()
            self.__logger__.setLevel(INFO)
        self.__logger__.info(message)
