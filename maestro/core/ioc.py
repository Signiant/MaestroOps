"""
ioc.py

Contains simple container class for Modules and the associated Exceptions.

"""


class SingleObjectContainer(object):
    """
    Simple IoC Container.

    You can init with a type, and the container will only accept those. Otherwise it will take the object type of the first registered object.

    Don't change the object_type manually, and don't reuse it.

    """

    __objects = None
    object_type = None

    def __init__(self, obj_type=None):
        if self.object_type is not None and not isinstance(Type, obj_type):
            raise TypeError("Expected type Type got " + str(type(obj_type)) + ".")
        self.object_type = obj_type
        self.__objects = dict()

    def register(self, id, obj):
        if obj is None:
            raise TypeError("You cannot register a None type.")

        if id is None:
            raise TypeError("You cannot use a None type as an id.")

        if not isinstance(id, str):
            raise TypeError("You must pass a valid string for the id!")

        if self.object_type is None:
            self.object_type = type(obj)

        if not isinstance(obj, self.object_type):
            raise TypeError("The object passed to the Container was not of type " + str(self.object_type) + ".")

        if id in list(self.__objects.keys()):
            raise ValueError("ID " + str(id) + " is already registered with this container.")

        self.__objects[id] = obj

    def deregister(self, id):
        """
        Deregisters object with id. Will raise ValueError if the id does not exist.
        """

        if id not in list(self.__objects.keys()):
            raise ValueError("ID " + str(id) + " is not registered with this container.")

        del self.__objects[id]

    def getinstance(self, id):
        """
        Returns a new instance of an object with the provided id. The container does not track this object.
        """
        if id not in list(self.__objects.keys()):
            raise ValueError("ID " + str(id) + " is not registered with this container.")
        item_type = type(self[id])

        return item_type()

    def unregister(self, id):
        return self.deregister(id)

    def __getitem__(self, id):
        if id not in list(self.__objects.keys()):
            raise KeyError("A " + str(self.object_type) + " object with id " + str(id) + " is not registered with this container.")

        return self.__objects[id]

    def get(self, id):
        return self.__getitem__(id)

    def list(self):
        return list(self.__objects.items())
